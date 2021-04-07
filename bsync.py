#!/usr/bin/env python3

#from sys import *
from copy import copy
from pickle import dumps,loads,HIGHEST_PROTOCOL
from threading import Thread,Condition,Lock
from os import open,close,write,read,O_NONBLOCK,pipe,pipe2,fork,getpid,wait
from time import sleep
from select import select
from abc import ABC
from struct import pack,unpack
from queue import PriorityQueue
from signal import signal,alarm,SIGALRM
import atexit

PICKLE_PROTO = 2;     # pickle protocol to use
TIMEOUT = 0.001;
DEFAULT_TAG = -1;
ROOT_RANK = 0;        # the controller rank
MAXMSGLEN = 1000; #1<<20;

# ========================================================================

class Xfer :
  """ A class for handing off messages between the loading queue
      (Loader.loading_loop) and the communication loop (comm_loop) """
  def __init__(self) :
    self._kill = False;
    self.msg = None;
    self.cv = Condition(Lock());
    self.ack = Condition(Lock());

class ProcessPool(object) :
  """ the pool of exec child processes """
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
    self.ack = Condition(Lock());   # acknowledge new proc available
    self.xfer = Xfer();
  def run_exec(self) :
    exec_loop(self);
  def get_ranks(self) :
    return [_[0] for _ in self.procs if _];      # list of ranks
  def get_avail_rank(self) :
    while True :
      n = self.nonblock_get_avail_rank();
      if not n is None : return n;
  def nonblock_get_avail_rank(self) :
    with self.lock :
      if not self.unused : return None;
      rank = self.unused.pop(0);
      self.used.append(rank);
      return rank;
  def return_avail_rank(self,p) :
    with self.lock :
      self.used.remove(p);
      self.unused.append(p);
    with self.ack :
      self.ack.notify_all();
  def avail(self) :
    with self.lock :
      return self.count;
  def __getitem__(self,k) :     # return the proc tuple for rank #k
    return self.procs[k];
  def deleter(self) :
    if not self.forked :      # only wait for child procs
      for i in self.procs :
        if not i is None : wait();
    self.forked = True;     # don't do the waiting again if we get here twice
  def __del__(self) :
    self.deleter();

class AsyncPool(object) :
  """ AsyncPool is a wrapper that encloses ProcessPool. The reason is so that
  we can present whatever interface we want to the outside world without
  mangling ProcessPool more than I would like. AsyncPool has a property
  called pool which is the instance of ProcessPool which gets instantiated
  when the AsyncPool is first called. Note that both AsyncPool and
  ProcessPool are singletons and will not allow multiple instantiation
  in a program. """
  def __init__(self,*v,**args) :
    self.pool = ProcessPool(*v,**args);
    if not self.ischild() :
      self.loader = Loader(self.pool);
      self.comm_thread = \
          Thread(target=comm_loop,args=(self.pool,));
      self.comm_thread.start();
      self.loader_thread = \
          Thread(target=self.loader.loading_loop);
      self.loader_thread.start();
    else :
      # Run the exec loop in the child process in the event
      # that the child is a forked process
      self.pool.run_exec();
      exit(0);
  def async(self,*v,**args) :
    return self.loader.load(*v,**args);
  def ischild(self) :
    return self.pool.forked;
  def deleter(self) :
    try :
      ch = self.ischild();
    except : return;
    if not ch :
      self.loader.kill();
      with self.pool.xfer.cv :
        self.pool.xfer.msg = KillMessage();
        self.pool.xfer.cv.notify_all();
      self.loader_thread.join();
      self.comm_thread.join();
      self.pool.deleter();
      del self.pool;
  def __del__(self) :
    self.deleter();
  def __enter__(self) :
    return self;
  def __exit__(self,type,value,tb) :
    self.deleter();
    return False;


class Request(ABC) :
  def __init__(self,k,msg=None,ready=False) :
    self.id = k;
    self.msg = msg;
    self.ready = ready;

class SendRequest(Request) : pass;

class RecvRequest(Request) : pass;

# =============== Message header manipulation routines

# each message has an 8-byte unsigned long integer at the start
# of it indicating how many bytes the message is (including the
# 8 bytes of the length field.) So, the smallest possible value
# for that first field would be 8 indicating a message payload
# of 0 bytes + 8 bytes for the length field

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

# =====================================================

class Status(object) :
  """ a wrapper class for a bool to provide for future enhancements """
  def __init__(self,b) : self._bool = b;
  def __bool__(self) : return self._bool;

class FakeFile(object) :
  """ a class that wraps and integer and provides a fileno method for access """
  def __init__(self,n) : self.fd = n;
  def __int__(self) : return self.fd;
  def fileno(self) : return self.fd;

def messg_isend(pool,rank,msg,tag=DEFAULT_TAG) :
  k,pid,rm,wm = pool[rank];
  # NOTE: Condition variables are not pickleable and so we have to excise
  #  them from the object before pickling them to send down the line. This
  # happens in a __getstate__ method in the Message superclass.
  if msg.raw is None :
    msg.raw = add_hdr(dumps(msg,protocol=PICKLE_PROTO));
  n = write(wm,msg.raw[:MAXMSGLEN]);
  if n :
    msg.raw = msg.raw[n:];
  return SendRequest(k,ready=not msg.raw);

def messg_send(pool,rank,msg,tag=DEFAULT_TAG) :
  while not messg_isend(pool,rank,msg,tag).ready : sleep(0);
  return True;

def messg_irecv(pool,rank,tag=DEFAULT_TAG) :
  k,pid,rm,wm = pool[rank];
  return RecvRequest(k);

def messg_recv(pool,rank,tag=DEFAULT_TAG) :
  req = messg_irecv(pool,rank,tag);
  k,pid,rm,wm = pool[req.id];
  while not messg_stat(pool,req) :
    sleep(TIMEOUT);
  req.msg = loads(req.msg);
  return req.msg;

def messg_stat(pool,req) :
  if type(req) is SendRequest :
    return Status(True);
  assert type(req) is RecvRequest;
  if req.ready : return True;
  k,pid,rd,wr = pool[req.id]
  r_rdy,w_rdy,e_rdy = select([FakeFile(rd),],[],[],0.);
  if r_rdy :
    # TODO: make max read length equal to remaining message characters
    data = read(rd,MAXMSGLEN);
    if req.msg is None :
      req.msg = data;
    else :
      req.msg += data;
    if len(req.msg) >= 8 and hdr_len(req.msg) == len(req.msg) :
      _,req.msg = remove_hdr(req.msg);
      req.ready = True;
  return req.ready;

def messg_get(pool,req) :
  while not messg_stat(pool,req) : sleep(0.);
  req.msg = loads(req.msg);
  return req.msg;

# ================= Message wrapping classes =====================

class Message(ABC) :
  """ abstract base class for messages to send or received """
  def __init__(self,rank,tag=DEFAULT_TAG) :
    self.rank = rank;
    self.tag = tag;
    self.req = None;
    self.raw = None;
    self.notified = False;
    self.cv = Condition(Lock());      # NOTE: can't be pickled
  def __getstate__(self) :      # remove cv property to allow pickling
    d = copy(self.__dict__);
    d['cv'] = None;
    return d;

class RecvMessage(Message) :
  """ a class representing a message to be received """
  def __init__(self,rank,tag=DEFAULT_TAG) :
    super(RecvMessage,self).__init__(rank,tag=tag);

class SendMessage(Message) :
  """ a class representing a message to be sent """
  def __init__(self,rank,msg,tag=DEFAULT_TAG) :
    self.msg = msg;
    super(SendMessage,self).__init__(rank,tag=tag);
  @property
  def func(self) : return self.msg[0];
  @property
  def args(self) : return self.msg[1:];

class ReturnMessage(Message) :
  """ a class for returning results from the exec_loop """
  def __init__(self,rank,msg,tag=DEFAULT_TAG) :
    self.msg = msg;
    super(ReturnMessage,self).__init__(rank,tag);

class ExecMessage(SendMessage) :
  """ a class for making execute requests of the exec_loop """
  def __init__(self,rank,f,*args,tag=DEFAULT_TAG,timeout=None) :
    try :
      dumps(f,protocol=PICKLE_PROTO);
    except :
      f = "__main__.%s.__name__" % f.__name__;
    self.timeout = timeout;
    super(ExecMessage,self).__init__(rank,(f,)+args,tag);

class KillMessage(Message) :
  """ a class indicating the exec_loop should shut down and exit """
  def __init__(self) :
    super(KillMessage,self).__init__(ROOT_RANK);

# =================================================================

def comm_loop(pool) :
  """ the main loop for handling communications with the child processes """
  polling = [];
  sending = [];
  xfer = pool.xfer;         # alias for the Xfer object
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
          for i in pool.get_ranks() :
            # NOTE: perhaps would be better to queue this up in sending list?
            x = copy(xfer.msg);
            messg_send(pool,i,x,x.tag);          # broadcast kill to everyone
          break;

      xfer.msg = None;  # OK, got this message so clear it for next one

      if polling :      # only if needed should we enter this
        toremove = [];  # list of connections to drop from the polling list
        for p in polling :
          if p.req is None :
            p.req = messg_irecv(pool,p.rank,p.tag);
          p.stat = messg_stat(pool,p.req);
          if p.stat :
            if not p.notified :
              with p.cv :
                if type(p) is RecvMessage :
                  p.msg = messg_get(pool,p.req);       # should not block here as msg is ready
                p.notified = True;
                pool.return_avail_rank(p.rank);
                p.cv.notify_all();
            toremove.append(p);             # don't change polling inside the loop
        for t in toremove :                 # now remove everyone that needs removing
          polling.remove(t);

      
      if sending :
        toremove = [];
        for s in sending :
          if s.req is None or not s.req.ready:
            s.req = messg_isend(pool,s.rank,s,s.tag);
            if not s.req.ready : continue;
          s.stat = messg_stat(pool,s.req);
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



class ProcException(Exception) :
  """ special exception that contains another exception for sending
    across the MPI connection """
  def __init__(self,e) :
    self.exc = e;

def sighandler(signum,frame) :      # sigalrm handler raises exception on timeout
  raise TimeoutError("Timeout in remote exec process");

# this is the execution loop on the remote end. All it does is receive commands
# and then executes them in a blocking fashion. If it receives a KillMessage,
# then drop out of the loop. Timeouts are 
def exec_loop(pool,rank=ROOT_RANK,tag=DEFAULT_TAG) :
  signal(SIGALRM,sighandler);       # arrange to capture SIGALRM
  while True :
    msg = messg_recv(pool,rank,tag);
    if type(msg) is KillMessage :     # OK, time to leave
      break;
    if type(msg) is ExecMessage :
      func = msg.func if not isinstance(msg.func,str) else eval(msg.func);
      timeout = getattr(msg,'timeout',None);
      try :
        try :
          if timeout : alarm(timeout);
          retval = func(*msg.args);
        except Exception as e :
          retval = ProcException(e);      # send an encapsulated exception
      # there is an unfortunate race here. It is possible for the exec'ed
      # function to raise an exception and then during the handling of that
      # exception for the timeout to occur which would then raise and clobber
      # the exec_loop for this rank with possibly downstream fatal and/or
      # unpredictable results. So, we double wrap the called function.
      except :
        # So, this should really be a very, very rare off-normal event.
        retval = Exception("Alarm timeout race in exec_loop");
      finally :
        if timeout : alarm(0);
      messg_send(pool,rank,ReturnMessage(rank,retval,tag),tag);
      del retval,func;

class MessageHandle(object) :
  def __init__(self,v,priority=0,timeout=None) :
    self.params = v;            # parameters for exec call (f,arg1,arg2,...)
    self.timeout=timeout;       # timeout for remote process (not for sending)
    self.priority = priority;   # priority of this task (lower number is *more* priority
  def attach(self,msg) : self.msg = msg;
  def ready(self) :
    return self.msg.notified if hasattr(self,'msg') else False;
  def get(self,reraise=True) :
    while not hasattr(self,'msg') : sleep(TIMEOUT);
    if not self.msg.notified :
      with self.msg.cv :
        self.msg.cv.wait_for(lambda : self.msg.notified);
    if reraise and isinstance(self.msg.msg.msg,ProcException) :
      raise self.msg.msg.msg.exc;
    return self.msg.msg.msg;
  def queued(self) :
    return hasattr(self.msg);
  def __lt__(self,other) :    # rank based on priority level
    return self.priority < other.priority;

class Loader(object) :
  def __init__(self,pool) :
    self.pool = pool;
    self._kill = False;
    self.lock = Lock();
    self._loading = PriorityQueue();
    self._killed = False;

  def load(self,*v,priority=0,timeout=None) :     # returns message handle to the job
    with self.lock :
      m = MessageHandle(v,priority=priority,timeout=timeout);
      self._loading.put(m);
    with self.pool.ack :
      self.pool.ack.notify_all();    # notify loadloop of new stuff
    return m;

  def kill(self) :
    with self.pool.ack :
      self._kill = True;
      self.pool.ack.notify_all();
    while not self._killed : pass;      # wait for empty queue

  def loading_loop(self) :
    xfer = self.pool.xfer;
    while not self._kill :
      with self.pool.ack :
        # NOTE: drops out at timeout even with none available
        #       This is needed in case of missed notify at startup, etc.
        self.pool.ack.wait_for(\
            lambda : (not self._loading.empty() \
                        and self.pool.avail()),timeout=0.1)

      if self._kill : break;

      nmax = self.pool.avail();
      if not nmax : continue;       # nobody is free now

      toremove = [];
      while True :
        with self.lock :      # only hold this lock for as short a time as possible
          if self._loading.empty() : break;
          n = self.pool.nonblock_get_avail_rank();
        if n is None : break;
        i = self._loading.get();      # will not block
        # load the command into the message queue
        with xfer.cv :
          xfer.ack.acquire();       # must release on all circumstances!
          xfer.msg = ExecMessage(n,*i.params,timeout=i.timeout);
          xfer.cv.notify();
        xfer.ack.wait_for(lambda : (xfer.msg is None));
        xfer.ack.release();
        # load the receiver of the return response into the message queue
        with xfer.cv :
          xfer.ack.acquire();       # must release on all circumstances!
          xfer.msg = RecvMessage(n);
          i.attach(xfer.msg);                  # attach message to MessageHandle
          xfer.cv.notify();
        xfer.ack.wait_for(lambda : (xfer.msg is None));
        xfer.ack.release();

    self._killed = True;      # indicator that no more tasks will be loaded

def myfunc(x) :
  print("@ myfunc got",x);
#  raise Exception("hell world!");
  return x+2;

#mypool = AsyncPool(3);

with AsyncPool(3) as mypool :
  if True :
    m1 = mypool.async(myfunc,4,timeout=1);
    m2 = mypool.async(myfunc,5);
    while not m1.ready() or not m2.ready() : sleep(0.01);
    print("@@ m1 ",m1.get(),m2.get());

  ms = [mypool.async(myfunc,i) for i in range(200)];
  while not all([_.ready() for _ in ms]) : sleep(0.1);
  for i in ms : print(i,i.get());


