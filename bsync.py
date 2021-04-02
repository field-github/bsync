#!/usr/bin/env python3

from sys import *
from pickle import *
from threading import *
from os import open,close,write,read,O_NONBLOCK,pipe,pipe2,fork,getpid,wait
from time import sleep
from select import select
from abc import ABC
from struct import pack,unpack

PICKLE_PROTO = 2;
TIMEOUT = 0.001;
DEFAULT_TAG = -1;
ROOT_RANK = 0;
MAXREAD = 1<<24;

# ========================================================================

class ProcessPool(object) :
  def __init__(self,n,tag=DEFAULT_TAG) :
    self.procs = [None,];
    self.unused = [];
    self.used = [];
    self.count = n;
    self.forked = False;
    for i in range(1,self.count+1) :
      rs,wm = pipe2(O_NONBLOCK);
      rm,ws = pipe2(O_NONBLOCK);
      pid = fork();
      if not pid :
        close(wm);close(rm);
        self.procs = [(ROOT_RANK,getpid(),rs,ws)];    # set fds for ROOT_RANK
        self.forked = True;
        return;
      else :
        close(ws);close(rs);
        # proc tuple is (rank#, pid, read file descriptor, write file descriptor)
        self.procs.append((i,pid,rm,wm));
        self.unused.append(i);
    self.lock = Lock();       # no need to fork that lock over and over
  def get_ranks(self) :
    return [_[0] for _ in self.procs if _];      # list of ranks
  def get_avail_rank(self) :
    with self.lock :
      while not self.unused : sleep(0);
      self.used.append(self.unused.pop(0));
  def return_avail_rank(self,p) :
    with self.lock :
      self.unused.append(p);
      self.used.remove(p);
  def avail(self) :
    with self.lock :
      return self.count;
  def __getitem__(self,k) :     # return the proc tuple for rank #k
    return self.procs[k];
  def __del__(self) :
    if not self.forked :      # only wait for child procs
      for i in self.procs :
        if not i is None : wait();
      print("@@@ done joining procpool");


class Request(ABC) :
  def __init__(self,k) :
    self.id = k;
    self.msg = None;
    self.ready = False;
class SendRequest(Request) : pass;
class RecvRequest(Request) : pass;

class Status(object) :
  def __init__(self,b) : self._bool = b;
  def __bool__(self) : return self._bool;

class FakeFile(object) :
  def __init__(self,n) : self.fd = n;
  def __int__(self) : return self.fd;
  def fileno(self) : return self.fd;

def mystat(req) :
  if type(req) is SendRequest : return Status(True);
  assert type(req) is RecvRequest;
  if req.ready : return True;
  k,pid,rd,wr = procpool[req.id]
  r_rdy,w_rdy,e_rdy = select([FakeFile(rd),],[],[],0.);
  if r_rdy :
    data = read(rd,MAXREAD);
    if req.msg is None :
      req.msg = data;
    else :
      req.msg += data;
    if len(req.msg) >= 8 and hdr_len(req.msg) == len(req.msg) :
      _,req.msg = remove_hdr(req.msg);
      req.ready = True;
  return req.ready;

def add_hdr(msg) :
  return pack('L',len(msg)+8)+msg;      # total length including count

def hdr_len(msg) :      # return just length of header
  assert(len(msg) >= 8);
  return unpack('L',msg[:8])[0];

def remove_hdr(msg) :
  assert(len(msg) >= 8);
  hdr = unpack('L',msg[:8])[0];
  msg = msg[8:];
  return hdr,msg;

def myisend(rank,msg,tag=DEFAULT_TAG) :
  k,pid,rm,wm = procpool[rank];
  # NOTE: Condition variables are not pickleable and so we have to excise
  #  them from the object before pickling them to send down the line
  if not hasattr(msg,'cv') :
    msg = dumps(msg,protocol=2);
  else :
    cv = msg.cv;
    msg.cv = None;
    tmp = dumps(msg,protocol=2);
    msg.cv = cv;
    msg = tmp;
  msg = add_hdr(msg);
  n = write(wm,msg);
  assert(n==len(msg));    # TODO must not break apart message for now
  return SendRequest(k);

mysend = myisend;     # ignore blocking send for now

def myirecv(rank,tag=DEFAULT_TAG) :
  k,pid,rm,wm = procpool[rank];
  return RecvRequest(k);

def myrecv(rank,tag=DEFAULT_TAG) :
  req = myirecv(rank,tag);
  k,pid,rm,wm = procpool[req.id];
  while not mystat(req) : sleep(0.);
  req.msg = loads(req.msg);
  return req.msg;

# ========================================================================

class Xfer :
  _inited = False;
  _kill = False;
  msg = None;
  def __init__(self) :
    assert not self._inited,"Xfer is a singleton";
    self.cv = Condition();
    self.ack = Condition();
xfer = Xfer();


class Rank(int) : pass;
class Tag(int) : pass;

class Message(object) :
  def __init__(self,rank,data=None,tag=DEFAULT_TAG) :
    self.rank = rank;
    self.data = data;
    self.tag = tag;
    self.req = None;
    self.notified = False;
    self.cv = Condition();      # NOTE: can't be pickled

class RecvMessage(Message) :
  def __init__(self,rank,tag=DEFAULT_TAG) :
    super(RecvMessage,self).__init__(rank,tag=tag);

class SendMessage(Message) :
  pickleable = set();                      # list of things known to be pickleable
  not_pickleable = set();                  # list of things known not to be pickleable
  def __init__(self,rank,msg,tag=DEFAULT_TAG) :
    if not msg[0] in self.pickleable :
      try :
        if msg[0] in self.not_pickleable : raise Exception();
        dumps(msg[0],protocol=2);       # attempt to pickle the function
        self.pickleable.add(msg[0]);
      except :
        # give it a string name if not pickleable and add to nonpickleable set
        self.not_pickleable.add(msg[0]);
        msg = ("__main__.%s.__name__" % msg[0].__name__,)+msg[1:];
    self.msg = msg;
    super(SendMessage,self).__init__(rank,tag=tag);
  @property
  def func(self) : return self.msg[0];
  @property
  def args(self) : return self.msg[1:];

class ExecMessage(SendMessage) :
  def __init__(self,rank,f,*args,tag=DEFAULT_TAG) :
    super(ExecMessage,self).__init__(rank,(f,)+args,tag);

class KillMessage(Message) :
  def __init__(self) :
    super(KillMessage,self).__init__(self,ROOT_RANK);

def comm_loop(ranks=[]) :
  polling = [];
  sending = [];
  with xfer.cv :
    while not xfer._kill :
      xfer.cv.wait_for(lambda :(xfer.msg or xfer._kill),timeout=TIMEOUT);
      if xfer._kill :                       # need to drop out now
        xfer.msg = KillMessage();
      must_ack = bool(xfer.msg);      # if there is a message, then we should acknowledge
      if must_ack :
        typ = type(xfer.msg);
        # NOTE: we're going to treat sending and receiving as different
        #  queues even though it seems like it might be possible to
        #  reuese code for both. In the future, I fear that the divergence
        #  between the two might increase and so this allows for that.
        if typ in [SendMessage,ExecMessage] :
          sending.append(xfer.msg);
        elif typ is RecvMessage :
          polling.append(xfer.msg);
        elif typ is KillMessage :
          for i in ranks :
            # NOTE: would it be better to queue this up in sending?
            MPI_Isend(i,xfer.msg);          # broadcast kill to everyone
          break;

      xfer.msg = None;  # OK, got this message so clear it for next one

      if polling :      # only if needed should we enter this
        toremove = [];  # list of connections to drop from the polling list
        for p in polling :
          if p.req is None :
            p.req = MPI_Irecv(p.rank,p.tag);
          p.stat = MPI_Stat(p.req);
          if p.stat :
            if not p.notified :
              with p.cv :
                if type(p) is RecvMessage :
                  p.msg = MPI_Get(p.req);       # should not block here as msg is ready
                p.notified = True;
                p.cv.notify_all();
            toremove.append(p);             # don't change polling inside the loop
        for t in toremove :                 # now remove everyone that needs removing
          polling.remove(t);

      if sending :
        toremove = [];
        for s in sending :
          if s.req is None :
            s.req = MPI_Isend(s.rank,s,s.tag);
          s.stat = MPI_Stat(s.req);
          if s.stat :
            if not s.notified :
              with s.cv :
                s.notified = True;
                s.cv.notify_all();
            toremove.append(s);
        for t in toremove :
          sending.remove(t);

      # acknowledge processing here
      if must_ack and not xfer.ack is None :
        with xfer.ack :
          xfer.ack.notify_all();



class MPILoop(object) :
  sending = [];
  _inited = False;
  def empty(self) :
    return not bool(self.sending);
  def addsend(self,req) :
    self.sending.append(req);
  def loop(self) :
    l = [i for i in self.sending if MPI_Stat(i.req)];      # list to remove
    for i in l : self.sending.remove(i);                   # remove sends that are done
  def __init__(self) :
    assert not self._inited,"MPILoop is a singleton";
mpiloop = MPILoop();

class MPImsg(object) :
  def __init__(self,req) :
    self.req = req;

def MPI_Isend(rank,msg,tag=DEFAULT_TAG) :
  return myisend(rank,msg,tag);

def MPI_Irecv(rank,tag=DEFAULT_TAG) :
  req = myirecv(rank,tag);
  return req;

def MPI_Stat(req) :
  return mystat(req);

def MPI_Get(req) :
  while not mystat(req) : sleep(0.);
  req.msg = loads(req.msg);
  return req.msg;


class ProcException(Exception) :
  def __init__(self,e) :
    self.exc = e;

# this is the execution loop on the remote end. All it does is receive commands
# and then executes them in a blocking fashion. If it receives a KillMessage,
# then drop out of the loop
def exec_loop(rank=ROOT_RANK,tag=DEFAULT_TAG) :
  while True :
    msg = myrecv(rank,tag);
    if type(msg) is KillMessage :
      break;
    if type(msg) is ExecMessage :
      func = msg.func if not isinstance(msg.func,str) else eval(msg.func);
      try :
        retval = func(*msg.args);
      except Exception as e :
        retval = ProcException(e);
      mysend(rank,retval,tag);
      del retval,func;


def myfunc(x) :
  print("@ myfunc got",x);
#  raise Exception("hell world!");
  return x+2;

procpool = ProcessPool(3);
if procpool.forked :
  exec_loop();
  del procpool;
  exit(0);
else :
  th = Thread(target=comm_loop,args=(procpool.get_ranks(),));
  th.start();

  for n in [1,2] :
    with xfer.cv :
      xfer.msg = ExecMessage(n,myfunc,3+n);
      xfer.ack.acquire();
      xfer.cv.notify();
    xfer.ack.wait_for(lambda : (xfer.msg is None));
    xfer.ack.release();
  
  t = [RecvMessage(n) for n in [1,2]];
  for n in t :
    with xfer.cv :
      xfer.ack.acquire();
      xfer.msg = n;
      xfer.cv.notify();
    xfer.ack.wait_for(lambda : (xfer.msg is None));
    xfer.ack.release();

  for i in t :
    while not i.notified :
      sleep(TIMEOUT);
    print("@ got it !! ",i.req.__dict__);

  if False :
    for n in [3,2,1] :
      t = RecvMessage(n);
      xfer.cv.acquire();
      xfer.msg = t;
      with t.cv :
        xfer.cv.release();
        while not t.notified : t.cv.wait();
        print("@ got out!!!! ",t.req.__dict__);
     
  if False :
    with xfer.cv :
      xfer.msg = ExecMessage(2,myfunc,13);
    sleep(0.1);
    t = RecvMessage(2);
    xfer.cv.acquire();
    xfer.msg = t;
    with t.cv :
      xfer.cv.release();
      while not t.notified : t.cv.wait();
      print("@ got out!!!! ",t.req.__dict__);

  with xfer.cv :
    xfer.msg = KillMessage();
  th.join();
  del procpool;

