#!/usr/bin/env python3

# UNTESTED ON MPI

import __main__
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
from numbers import Integral
from functools import singledispatch

PICKLE_PROTO = 2;     # pickle protocol to use
TIMEOUT = 0.001;
DEFAULT_TAG = -1;
ROOT_RANK = 0;        # the controller rank
MAXPACKETLEN = 1000; #1<<20;

try :
  from mpi4py import MPI
  comm = MPI.COMM_WORLD;
  mpi_rank = comm.Get_rank();
  mpi_size = comm.Get_size();
  use_mpi = getattr(__main__,'bsync_use_mpi',True);     # default to MPI if succeed
except :
  use_mpi = False;

# ========================================================================

class Xfer :
  """ A class for handing off messages between the loading queue
      (Loader.loading_loop) and the communication loop (comm_loop) """
  def __init__(self) :
    self._kill = False;
    self.msg = None;
    self.cv = Condition(Lock());
    self.ack = Condition(Lock());

class ProcessHandle(ABC) :
  def __init__(self,**args) :
    for k,v in args.items() :
      self.__dict__[k] = v;
  @property
  def rank(self) : return self.rank;

class ForkProcessHandle(ProcessHandle) :
  def __init__(self,rank,pid,rd,wr) :
    super(ForkProcessHandle,self).__init__(rank=rank,pid=pid,rd=rd,wr=wr);
  @property
  def rwfileno(self) : return self.rd,self.wr;
  @property
  def pid(self) : return self.pid;

class MPIProcessHandle(ProcessHandle) :
  def __init__(self,rank) :
    super(MPIProcessHandle,self).__init__(rank=rank);

class ProcessPool(object) :
  """ the pool of exec child processes """
  def __init__(self,n=None,tag=DEFAULT_TAG,use_fork=False) :
    """ initialize process pool.
        NOTE: tag is only used in MPI communications, not for forked processes
      __init__
        Args:
          n(int or list/tuple):       number of processes OR list of ranks to use
          tag(int):                   MPI tag to use in communications
    """
    self.procs = [];
    self.unused = [];
    self.used = [];
    self.using_mpi = False;

    if use_mpi and not use_fork :
      # use a provided list of ranks if present, otherwise try to size to
      # the mpi ranks size *or* the integer value provided whichever is smaller
      if n is None : n = mpi_size;
      if isinstance(n,Integral) :
        n = range(1,min(n,mpi_size));
      self.using_mpi = True;

    # massage n so that it is a list with the ranks, and set the count
    if isinstance(n,Integral) :
      self.count = n;
      n = range(n);
    else :
      self.count = len(n);

    self.forked = False;

    if not self.using_mpi :      # forked process version
      for i in n :
        rs,wm = pipe2(O_NONBLOCK);
        rm,ws = pipe2(O_NONBLOCK);
        pid = fork();
        if not pid :
          close(wm);close(rm);
          self.procs = [ForkProcessHandle(ROOT_RANK,getpid(),rs,ws),];
          self.forked = True;
          return;
        else :
          close(ws);close(rs);
          # proc tuple is (rank#, pid, read file descriptor, write file descriptor)
          self.procs.append(ForkProcessHandle(i,getpid(),rm,wm));
          self.unused.append(i);
    else :        # MPI version
      if mpi_rank == ROOT_RANK :
        self.procs = [MPIProcessHandle(_) for _ in n];
      else :
        self.procs = [MPIProcessHandle(ROOT_RANK),];

    self.lock = Lock();       # no need to fork that lock over and over
    self.ack = Condition(Lock());   # acknowledge new proc available
    self.xfer = Xfer();
  def run_exec(self) :
    exec_loop(self[0]);       # exec_loop runs in child process with only one ProcessHandle
  def get_size(self) :
    return list(range(len(self.procs)));      # list of ranks
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
  def __init__(self,proc,msg=None,ready=False) :
    self.proc = proc;
    self.msg = msg;
    if not ready is None :
      # MPI requests use test() method to generate ready flag
      self.ready = ready;

class SendRequest(Request) : pass;

class RecvRequest(Request) : pass;

class MPIRequest(Request) :
  """ a wrapper for mpi4py's Request method. It provides the procno of
      the cpu being communicated with as well as a ready property
      which tests for complete communication.
      Properties:
        proc(ProcessHandle):        Contains the rank to communicate with
        msg(Message):               the received message
        mpireq(MPIRequest):         mpi4py MPIRequest object
  """
  def __init__(self,mpireq,proc,msg=None,ready=False) :
    self.mpireq = mpireq;
    super(MPIRequest,self).__init__(proc,ready=None);
  @property
  def ready(self) :
    return self.mpireq.test();            # return the test for done

class MPIRecvRequest(MPIRequest) : pass;

class MPISendRequest(MPIRequest) : pass;

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

class FakeFile(object) :
  """ a class that wraps and integer and provides a fileno method for access """
  def __init__(self,n) : self.fd = n;
  def __int__(self) : return self.fd;
  def fileno(self) : return self.fd;

# ------------- dispatch control by child type MPI or forked -----------

# NOTE: type of poolproc argument is
#               ForkProcessHandle       <--- forked child subprocess on same machine
#               MPIProcessHandle        <--- MPI rank process perhaps on another node
#
#       type of req argument is either RecvMessage or SendMessage

@singledispatch
def messg_isend(poolproc,msg,tag=DEFAULT_TAG) :
  raise NotImplementedError("Unsupported type: %s" % type(poolproc));

@singledispatch
def messg_send(poolproc,msg,tag=DEFAULT_TAG) :
  raise NotImplementedError("Unsupported type: %s" % type(poolproc));

@singledispatch
def messg_irecv(poolproc,tag=DEFAULT_TAG) :
  raise NotImplementedError("Unsupported type: %s" % type(poolproc));

@singledispatch
def messg_recv(poolproc,tag=DEFAULT_TAG) :
  """ messg_recv is different than messg_get in that it creates
      the receive request. messg_get is used when you already have
      the receive request and you want to continue the receiving
      operation. This will block until the entire message is received. """
  raise NotImplementedError("Unsupported type: %s" % type(poolproc));

@singledispatch
def messg_stat(poolproc,req) :
  raise NotImplementedError("Unsupported type: %s" % type(poolproc));

@singledispatch
def messg_get(poolproc,req) :
  """ messg_get is different than message_recv in that it assumes
      you already have a request object for this receive. If you
      don't have a request object yet, then use messg_recv. But,
      like messg_recv, it will block until the entire message is received. """
  raise NotImplementedError("Unsupported type: %s" % type(poolproc));

# ------------ forked child communication primitives --------------------
#   isend, send         <--- unblocking and blocking send
#   irecv, recv         <--- unblocking and blocking recv
#   stat                <---- check on the received status of isend or irecv
#   get, [not implemented] put      <--- blocking wait for full receive
#
# NOTE:  isend and irecv return Request objects which only indicate that
#  a sending/receiving operation has started. The actual writing and
#  reading takes place in the appropriate messg_stat function for the
#  given procpool type

@messg_isend.register(ForkProcessHandle)
def _(poolproc,msg,tag=DEFAULT_TAG) :
  rm,wm = poolproc.rwfileno;
  # NOTE: Condition variables are not pickleable and so we have to excise
  #  them from the object before pickling them to send down the line. This
  # happens in a __getstate__ method in the Message superclass.
  if msg.raw is None :
    msg.raw = add_hdr(dumps(msg,protocol=PICKLE_PROTO));
  return SendRequest(poolproc,msg=msg,ready=not msg.raw);

@messg_send.register(ForkProcessHandle)
def _(poolproc,msg,tag=DEFAULT_TAG) :
  """ blocking send. """
  req = messg_isend(poolproc,msg,tag);
  while not req.ready :
    while not messg_stat(poolproc,req) :
      sleep(0);
  return True;

@messg_irecv.register(ForkProcessHandle)
def _(poolproc,tag=DEFAULT_TAG) :
  return RecvRequest(poolproc);

@messg_recv.register(ForkProcessHandle)
def _(poolproc,tag=DEFAULT_TAG) :
  req = messg_irecv(poolproc,tag);
  return messg_get(poolproc,req);

@messg_stat.register(ForkProcessHandle)
def _(poolproc,req) :
  assert type(req) in [RecvRequest,SendRequest],\
              "messg_stat received incorrect type %s" % str(type(req));

  if req.ready :        # no reason to do anything if already done
    return True;

  rd,wr = poolproc.rwfileno;
  if type(req) is SendRequest :       # ===== SEND OPTION

    msg = req.msg;
    while True :
      r_rdy,w_rdy,e_rdy = select([],[FakeFile(wr),],[],0.);
      if not w_rdy : break;
      n = write(wr,msg.raw[:MAXPACKETLEN]);
      if n :
        msg.raw = msg.raw[n:];
      if not msg.raw : break;         # written the whole message now
    req.ready = not msg.raw;

  else :                              # ===== RECEIVE OPTION

    while True :
      r_rdy,w_rdy,e_rdy = select([FakeFile(rd),],[],[],0.);
      if not r_rdy : break;
      # NOTE: We peek at the 8-byte length header before allowing
      #  the read of the rest of the message. In this way, no matter
      #  what the OS does as far as combining or splitting up messages
      #  we won't ever read past the end of one message into the next.
      if req.msg is None :
        mxlen = 8;
      elif len(req.msg) < 8 :
        mxlen = 8-len(req.msg)
      else :
        mxlen = min(hdr_len(req.msg),MAXPACKETLEN);
      data = read(rd,mxlen);
      if req.msg is None :
        req.msg = data;
      else :
        req.msg += data;
      if len(req.msg) >= 8 and hdr_len(req.msg) == len(req.msg) :
        _,req.msg = remove_hdr(req.msg);
        req.ready = True;
        break;

  return req.ready;

@messg_get.register(ForkProcessHandle)
def _(poolproc,req) :
  while not messg_stat(poolproc,req) : sleep(TIMEOUT);
  req.msg = loads(req.msg);
  return req.msg;

# ---------------- MPI Communication primitives ----------------------------

@messg_isend.register(MPIProcessHandle)
def _(poolproc,msg,tag=DEFAULT_TAG) :
  req = comm.isend(msg,dest=poolproc.rank,tag=tag);
  return MPISendRequest(req);

@messg_send.register(MPIProcessHandle)
def _(poolproc,msg,tag=DEFAULT_TAG) :
  comm.send(msg,dest=poolproc.rank,tag=tag);
  return True;

@messg_irecv.register(MPIProcessHandle)
def _(poolproc,tag=DEFAULT_TAG) :
  req = comm.irecv(source=poolproc.rank,tag=tag);
  return MPIRecvRequest(req);

@messg_recv.register(MPIProcessHandle)
def _(poolproc,tag=DEFAULT_TAG) :
  data = comm.recv(source=poolproc.rank,tag=tag);
  return data;

@messg_stat.register(MPIProcessHandle)
def _(poolproc,req) :
  if not req.mpireq.ready :
    return False;
  if type(req) is MPIRecvRequest :
    # acquire the communication result message. This should not
    # block here
    req.msg = req.mpireq.wait();
  return True;

@messg_get.register(MPIProcessHandle)
def _(poolproc,req) :
  # acquire the result and set msg to it. Then return the message also.
  req.msg = req.mpireq.wait();
  return req.msg;

# --------------------------------------------------------------------

# ================= Message wrapping classes =====================

class Message(ABC) :
  """ abstract base class for messages to send or received """
  def __init__(self,procno,tag=DEFAULT_TAG) :
    self.procno = procno;
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
  def __init__(self,procno,tag=DEFAULT_TAG) :
    super(RecvMessage,self).__init__(procno,tag=tag);

class SendMessage(Message) :
  """ a class representing a message to be sent """
  def __init__(self,procno,msg,tag=DEFAULT_TAG) :
    self.msg = msg;
    super(SendMessage,self).__init__(procno,tag=tag);
  @property
  def func(self) : return self.msg[0];
  @property
  def args(self) : return self.msg[1:];

class ReturnMessage(Message) :
  """ a class for returning results from the exec_loop """
  def __init__(self,procno,msg,tag=DEFAULT_TAG) :
    self.msg = msg;
    super(ReturnMessage,self).__init__(procno,tag);

class ExecMessage(SendMessage) :
  """ a class for making execute requests of the exec_loop """
  def __init__(self,procno,f,*args,tag=DEFAULT_TAG,timeout=None) :
    try :
      dumps(f,protocol=PICKLE_PROTO);
    except :
      f = "__main__.%s.__name__" % f.__name__;
    self.timeout = timeout;
    super(ExecMessage,self).__init__(procno,(f,)+args,tag);

class KillMessage(Message) :
  """ a class indicating the exec_loop should shut down and exit """
  def __init__(self) :
    super(KillMessage,self).__init__(ROOT_RANK);

# =================================================================

def comm_loop(pool) :
  """ the main loop for handling communications with the child processes.
    Note that MPI implementations depend on non-thread safe code and therefore
    it is necessary that all communications be aggregated into a single thread.
    Therefore, we run a separate event loop that handles the communications
    from the loop which queues up the tasks. The task loop is in class Loader.
    One could combine the task event loop with the communications loop, but
    that would likely hurt latency because you could have thousands of tasks
    queued up when only a few communications operations were active at any
    given time.

    Args:
      pool(ProcessPool):      the pool of processes to use for executing tasks
  """
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
          for i in pool.get_size() :
            # NOTE: perhaps would be better to queue this up in sending list?
            x = copy(xfer.msg);
            messg_send(pool[i],x,x.tag);          # broadcast kill to everyone
          break;

      xfer.msg = None;  # OK, got this message so clear it for next one

      if polling :      # only if needed should we enter this
        toremove = [];  # list of connections to drop from the polling list
        for p in polling :
          if p.req is None :
            p.req = messg_irecv(pool[p.procno],p.tag);
          p.stat = messg_stat(pool[p.procno],p.req);
          if p.stat :
            if not p.notified :
              with p.cv :
                if type(p) is RecvMessage :
                  p.msg = messg_get(pool[p.procno],p.req);       # should not block here as msg is ready
                p.notified = True;
                pool.return_avail_rank(p.procno);
                p.cv.notify_all();
            toremove.append(p);             # don't change polling inside the loop
        for t in toremove :                 # now remove everyone that needs removing
          polling.remove(t);

      
      if sending :
        toremove = [];
        for s in sending :
          if s.req is None or not s.req.ready:
            s.req = messg_isend(pool[s.procno],s,s.tag);
          s.stat = messg_stat(pool[s.procno],s.req);
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
    across the MPI connection 
    Args:
      e(Exception):   the exception that is to be encapsulated
    
  """
  def __init__(self,e) :
    self.exc = e;

def sighandler(signum,frame) :      # sigalrm handler raises exception on timeout
  """ alarm signal handler. Just raises a TimeoutError. """
  raise TimeoutError("Timeout in remote exec process");

# this is the execution loop on the remote end. All it does is receive commands
# and then executes them in a blocking fashion. If it receives a KillMessage,
# then drop out of the loop. Timeouts are 
def exec_loop(poolproc,tag=DEFAULT_TAG) :
  """ exec_loop is the loop on the child process that actually executes tasks.
    Args:
      pool(ProcessHandle):      The process handle indicating how to communicate
                                back to the master controller
      rank(int):                (not used for now) The rank of the master controller
      tag(int):                 The tag to send in MPI communications
  """
  signal(SIGALRM,sighandler);       # arrange to capture SIGALRM
  while True :
    msg = messg_recv(poolproc,tag);
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
      messg_send(poolproc,ReturnMessage(None,retval,tag),tag);    # NOTE: rank is unused here
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

__all__ = ["AsyncPool",];


