#!/usr/bin/env python3

"""
  bsync

  A replacement for async.

  Provides MPI thread pool for execution. Based on mpi4py (or equivalent
  mpi4pylite).

  SEE README.md

  SYNOPSIS:

    from bsync import *
    with AsyncPool() as mypool :
      # call myfunc(i) but on a remote MPI process
      ms = [mypool.async(myfunc,i,[timeout=1],[priority=0]) for i in range(20)];
      # [OPTIONAL] block while polling for ready
      while not all([_.ready() for _ in ms]) : sleep(1);
      # This will block until everybody is ready even if no ready test is made
      print([_.get(reraise=True) for _ in ms]);

  Higher priority is given to smaller priority number. Default priority is zero,
  so if you want *more* priority, use a negative number.

  The reraise keyword defaults to True in which case exceptions in the remote
  MPI task will be re-raised as exceptions at the .get() method in the
  controlling process. If you turn reraise=False, then those exceptions
  will return a ProcException that contains the remote exception ins self.exc.

  There is also a .ready() method to apply to the async() returned object
  to return a bool test for exec'ed complete.

  If MPI is not available, this module will default to use subprocess pipe/fork
  instead.

  EXTRAS

    AsyncPool has some keyword options:

      keep_unused=True        unused mpi ranks will drop through AsyncPool so
                              they can be used for other MPI actions not
                              involving the thread pool.

    __main__.bsync_use_mpi = False      Turn off MPI and use pipe/fork for
                                        subprocesses.

"""

import sys
import __main__
from copy import copy
from pickle import dumps,loads,HIGHEST_PROTOCOL
from threading import Thread,Condition,Lock
import os
from os import close,write,read,O_NONBLOCK,pipe,pipe2,fork,\
                getpid,wait,popen,waitpid
from fcntl import fcntl,F_SETFD
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
DEFAULT_TAG = 0;
ROOT_RANK = 0;        # the controller rank
MAXPACKETLEN = 1<<24; # maximum chunks to break up messages into

# Some dumb logic to try and figure out whether to use MPI by default
try :
  from mpi4py import MPI
  comm = MPI.COMM_WORLD;
  mpi_rank = comm.Get_rank();
  mpi_size = comm.Get_size();
  if mpi_size > 1 :
    use_mpi = getattr(__main__,'bsync_use_mpi',True);     # default to MPI if succeed
  else :
    use_mpi = False;
except :
  use_mpi = False;


# ========================================================================

def bsync_using_mpi() :
  """ return True if operating in MPI mode """
  return use_mpi;

def bsync_get_rank() :
  """ return the MPI rank (if using MPI) """
  if bsync_using_mpi :
    return mpi_rank;
  else :
    return None;

class Xfer :
  """ A class for handing off messages between the loading queue
      (Loader.loading_loop) and the communication loop (comm_loop) """
  def __init__(self,tag=DEFAULT_TAG) :
    self._kill = False;
    self.msg = None;
    self.cv = Condition(Lock());
    self.ack = Condition(Lock());
    self.tag = tag;

class ProcessHandle(ABC) :
  """ A class that describes a slave task - either MPI or Forked subclasses """
  def __init__(self,**args) :
    for k,v in args.items() :
      self.__dict__[k] = v;
  @property
  def rank(self) : return self._rank;

class ForkProcessHandle(ProcessHandle) :
  def __init__(self,rank,pid,rd,wr) :
    super(ForkProcessHandle,self).__init__(rank=rank,_pid=pid,rd=rd,wr=wr);
  @property
  def rwfileno(self) : return self.rd,self.wr;      # file descriptors for r,w pipes
  @property
  def pid(self) : return self._pid;     # PID of the slave task

class MPIProcessHandle(ProcessHandle) :
  def __init__(self,rank) :
    super(MPIProcessHandle,self).__init__(_rank=rank);

def get_num_cpus() :
  """ Return number of cpus on the machine for default forking operation """
  try :
    with popen("/usr/bin/nproc") as f :   # POSIX command for number of cores
      return int(f.readline());
  except :
    try :   # MAC OS X equivalent of nproc command
      with popen("/usr/sbin/sysctl -n hw.logicalcpu") as f :
        return int(f.readline());
    except :
      print("*** can't find nproc to determine number of cpus on node ***",file=sys.stderr);
  print("*** going to return 4 cpus since I have no idea how many is right ***");
  return 4;

class ProcessPool(object) :
  """ the pool of exec child processes """
  def __init__(self,n=None,tag=DEFAULT_TAG,use_fork=False,**args) :
    """ initialize process pool.
        NOTE: tag is only used in MPI communications, not for forked processes
      __init__
        Args:
          n(int or list/tuple):       number of processes OR list of ranks to use
                                      If a list, then a None element indicates a
                                      forked subprocess. So it is possible to
                                      mix fork and MPI processes.
          tag(int):                   MPI tag to use in communications
    """
    self.procs = [];
    self.unused = [];
    self.used = [];
    self.xfer = Xfer(tag=tag);      # NOTE: needs tag even in child processes
    # these two flags indicate whether this is MPI, whether a subprocess and whether
    # there should *not* be a exec_loop run for the task
    self.using_mpi = False;
    self._ischild = False;
    self.no_bsync = False;

    # this flag specifies whether we are MPI enabled
    # if not, *all* processes will be forked. Also,
    # using_mpi does *not* indicate that *any* mpi ranks will
    # actually be assigned to exec slave tasks
    self.using_mpi = use_mpi and not use_fork;

    # do some massaging of the n argument to make it into a list of integers
    if self.using_mpi :
      if n is None : n = mpi_size-1;
      if isinstance(n,Integral) :
        n = range(1,min(n+1,mpi_size));     # all remaining ranks (or n)
    else :   # not MPI
      ncpu = get_num_cpus();
      if n is None : n = ncpu-1;
      if isinstance(n,Integral) :
        n = range(1,min(n+1,ncpu));     # all remaining ranks (or n)

    # the number of subprocesses total
    self.count = len(n);
    self.unused = [_ for _ in range(self.count)];

    if self.using_mpi and mpi_rank == ROOT_RANK :
      assert not ROOT_RANK in n,"Can't use root rank as a child mpi task process";
      self.procs = [MPIProcessHandle(_) for _ in n if not _ is None];

    # mark all the unallocated task ranks as such
    if self.using_mpi and mpi_rank != ROOT_RANK :
      if not mpi_rank in n :
        self.no_bsync = True;
        self._ischild = True;

    for ii,i in enumerate(n) :
      if not self.using_mpi or mpi_rank == ROOT_RANK :
        if not self.using_mpi or i is None :    # need to fork a subprocess
          rs,wm = pipe();
          rm,ws = pipe();
          for fd in [rs,wm,rm,ws] : fcntl(fd,F_SETFD,O_NONBLOCK);
          pid = fork();
          if not pid :
            close(wm);close(rm);
            self.procs = [ForkProcessHandle(ROOT_RANK,getpid(),rs,ws),];
            self._ischild = True;
            return;
          else :
            close(ws);close(rs);
            self.procs.append(ForkProcessHandle(i,pid,rm,wm));
      else :      # use MPI for this task
        if mpi_rank != ROOT_RANK and i == mpi_rank :
          self._ischild = True;
          self.procs = [MPIProcessHandle(ROOT_RANK),];
          self.no_bsync = False;

    # NOTE: no need to make these for the subprocesses
    if not self._ischild :
      self.lock = Lock();             # no need to fork that lock over and over
      self.ack = Condition(Lock());   # acknowledge new proc available

    return;                           # end __init__

  def run_exec(self) :
    exec_loop(self[0],self.xfer.tag);         # exec_loop runs in child process with only one ProcessHandle
    if isinstance(self[0],ForkProcessHandle) :
      for fd in self[0].rwfileno : close(fd); # close the pipes after done
  def get_size(self) :
    return len(self.procs);
  def get_ranks(self) :
    return [_.rank for _ in self.procs];
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
    if not self._ischild :      # only wait for child procs
      self._ischild = True;     # don't do the waiting again if we get here twice
      for i in self.procs :
        if isinstance(i,ForkProcessHandle) :
          for fd in i.rwfileno : close(fd);
          waitpid(i.pid,0);
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
      # that the child is a forked process. For mpi, we may
      # not use all the ranks and so for those, we exit here.
      if not self.pool.no_bsync :
        self.pool.run_exec();
        sys.exit(0) if isinstance(self.pool[0],MPIProcessHandle) else os._exit(0);
      else :
        if args.get('keep_unused',False) :      # drop through rank for this one
          return;       # allow unused ranks to drop through back to user control
      sys.exit(0);              # normal python exit for MPI (unused rank)
  def get_size(self) :
    return self.pool.get_size();
  def async(self,*v,**args) :
    return self.loader.load(*v,**args);
  def ischild(self) :
    """ test to see if this process is a task executing child process.
      Returns:
        bool:           True if a child process
        None:           if the ProcessPool has already been deleted in
                        final program exit
    """
    try :
      return self.pool._ischild;
    except :
      return None;
  def deleter(self) :
    if self.ischild() == False :   # None is a possible case (on final deletion) too
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
    """ Returns:
          bool:       True or False depending on if transaction is complete.
                      Note that when message is received, req.msg is updated to
                      include the message. Also note that the behavior of this
                      is different than for forked pipe processes in which ready
                      property is literally just a settable bool and does *not*
                      update the message. The reason for this clumsy arrangement
                      is that mpi4py returns the message on a test() operation
                      and then refuses to return the message again if asked,
                      therefore the message needs to be cached upon the ready
                      call - but only in mpi mode.
    """
    if not self.msg is None :
      return True;
    tst = self.mpireq.test();
    if not tst[0] :             # test for error condition
      return False;
    else :
      if tst[1] :               # test for actual returned message
        self.msg = tst[1];      # NOTE: None is not a legal message
        return True;
      return False;            # return the result now

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
  assert not msg is None,"Can't send a None message";
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
  assert not msg is None,"Can't send a None message";
  req = messg_isend(poolproc,msg,tag);
  while not req.ready :
    while not messg_stat(poolproc,req,timeout=1) :
      sleep(TIMEOUT);
  return True;

@messg_irecv.register(ForkProcessHandle)
def _(poolproc,tag=DEFAULT_TAG) :
  return RecvRequest(poolproc);

@messg_recv.register(ForkProcessHandle)
def _(poolproc,tag=DEFAULT_TAG) :
  req = messg_irecv(poolproc,tag);
  return messg_get(poolproc,req);

@messg_stat.register(ForkProcessHandle)
def _(poolproc,req,timeout=0.) :
  """ read timeout can be specified if desired. write timeout is always zero for stat """
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
      r_rdy,w_rdy,e_rdy = select([FakeFile(rd),],[],[],timeout);
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
  while not messg_stat(poolproc,req,timeout=1.) :
    sleep(TIMEOUT);
  req.msg = loads(req.msg);
  return req.msg;

# ---------------- MPI Communication primitives ----------------------------

@messg_isend.register(MPIProcessHandle)
def _(poolproc,msg,tag=DEFAULT_TAG) :
  assert not msg is None,"Can't send a None message";
  req = comm.isend(msg,dest=poolproc.rank,tag=tag);
  return MPISendRequest(req,poolproc);

@messg_send.register(MPIProcessHandle)
def _(poolproc,msg,tag=DEFAULT_TAG) :
  assert not msg is None,"Can't send a None message";
  comm.send(msg,dest=poolproc.rank,tag=tag);
  return True;

@messg_irecv.register(MPIProcessHandle)
def _(poolproc,tag=DEFAULT_TAG) :
  req = comm.irecv(source=poolproc.rank,tag=tag);
  return MPIRecvRequest(req,poolproc);

@messg_recv.register(MPIProcessHandle)
def _(poolproc,tag=DEFAULT_TAG) :
  data = comm.recv(source=poolproc.rank,tag=tag);
  return data;

@messg_stat.register(MPIProcessHandle)
def _(poolproc,req) :
  return req.ready;

@messg_get.register(MPIProcessHandle)
def _(poolproc,req) :
  # acquire the result and set msg to it. Then return the message also.
  if not req.msg is None :
    return req.msg;
  req.msg = req.mpireq.wait();
  return req.msg;

# --------------------------------------------------------------------

# ================= Message wrapping classes =====================

class Message(ABC) :
  """ abstract base class for messages to send or received """
  def __init__(self,procno,cv=None) :
    self.procno = procno;
    self.req = None;
    self.raw = None;
    self.notified = False;
    self.cv = cv if cv else Condition(Lock());      # NOTE: can't be pickled
  def __getstate__(self) :      # remove cv property to allow pickling
    d = copy(self.__dict__);
    d['cv'] = None;
    return d;

class RecvMessage(Message) :
  """ a class representing a message to be received """
  def __init__(self,procno,cv=None) :
    super(RecvMessage,self).__init__(procno,cv=cv);

class SendMessage(Message) :
  """ a class representing a message to be sent """
  def __init__(self,procno,msg) :
    self.msg = msg;
    super(SendMessage,self).__init__(procno);
  @property
  def func(self) : return self.msg[0];
  @property
  def args(self) : return self.msg[1:];

class ReturnMessage(Message) :
  """ a class for returning results from the exec_loop """
  def __init__(self,procno,msg) :
    self.msg = msg;
    super(ReturnMessage,self).__init__(procno);

class ExecMessage(SendMessage) :
  """ a class for making execute requests of the exec_loop """
  def __init__(self,procno,f,*args,timeout=None) :
    try :
      dumps(f,protocol=PICKLE_PROTO);
    except :
      f = "__main__.%s.__name__" % f.__name__;
    self.timeout = timeout;
    super(ExecMessage,self).__init__(procno,(f,)+args);

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
          for i in range(pool.get_size()) :
            # NOTE: perhaps would be better to queue this up in sending list?
            x = copy(xfer.msg);
            messg_send(pool[i],x,xfer.tag);          # broadcast kill to everyone
          break;

      xfer.msg = None;  # OK, got this message so clear it for next one

      if polling :      # only if needed should we enter this
        toremove = [];  # list of connections to drop from the polling list
        for p in polling :
          if p.req is None :
            p.req = messg_irecv(pool[p.procno],xfer.tag);
          p.stat = messg_stat(pool[p.procno],p.req);
          if p.stat :
            if not p.notified :
              with p.cv :
                if type(p) is RecvMessage :
                  p.msg = messg_get(pool[p.procno],p.req);       # should not block here as msg is ready
                  if isinstance(p.msg,Message) : p.msg = p.msg.msg;
                p.notified = True;
                pool.return_avail_rank(p.procno);
                p.cv.notify_all();
            toremove.append(p);             # don't change polling inside the loop
        for t in toremove :                 # now remove everyone that needs removing
          polling.remove(t);

      
      if sending :
        toremove = [];
        for s in sending :
          if s.req is None :
            s.req = messg_isend(pool[s.procno],s,xfer.tag);
          # NOTE: for MPI, we assume an isend is on its way, no need to poll
          s.stat = messg_stat(pool[s.procno],s.req) \
                        if isinstance(pool[s.procno],ForkProcessHandle) else True;
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
          if timeout : alarm(int(timeout));
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
      messg_send(poolproc,ReturnMessage(None,retval),tag);    # NOTE: rank is unused here
      del retval,func;

class MessageHandle(object) :
  def __init__(self,v,priority=0,timeout=None) :
    self.params = v;            # parameters for exec call (f,arg1,arg2,...)
    self.timeout=timeout;       # timeout for remote process (not for sending)
    self.priority = priority;   # priority of this task (lower number is *more* priority
    self.cv = Condition(Lock());  # This Condition() is forwarded on to the message
  def attach(self,msg) : self.msg = msg;
  def ready(self) :
    return self.msg.notified if hasattr(self,'msg') else False;
  def get(self,reraise=True) :
    # NOTE: self.cv is also copied into the message object. We use our
    #   local handle to self.cv here, but it will be notified in the comm_loop
    #   by its handle copy in the RecvMessage instance for this task.
    #   Until this happens, this MessageHandle instance will not have a
    #   self.msg property, so we have to test for the presence of self.msg
    #   as well as the notified flag in that self.msg which only appears
    #   once the task has been sent for execution.
    if not hasattr(self,'msg') or not self.msg.notified :
      with self.cv :
        self.cv.wait_for(lambda : hasattr(self,'msg') and self.msg.notified);
    if reraise and isinstance(self.msg.msg,ProcException) :
      raise self.msg.msg.exc;
    return self.msg.msg;
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
          xfer.msg = RecvMessage(n,cv=i.cv);
          i.attach(xfer.msg);                  # attach message to MessageHandle
          xfer.cv.notify();
        xfer.ack.wait_for(lambda : (xfer.msg is None));
        xfer.ack.release();

    self._killed = True;      # indicator that no more tasks will be loaded

__all__ = ["AsyncPool","bsync_using_mpi","bsync_get_rank"];
if use_mpi : __all__ += ['mpi_rank','mpi_size'];

def myhellfunc(arg) :
  return "Hell world, %s!" % arg;

def myrandfunc(a,b,c) :
  return random((c,c))*b+a;

def mytimeout(t) :
  sleep(t);
  return True;

if __name__ == "__main__" :
  from numpy.random import *
  
  fprint = lambda *a,**b:print(*a,**b,file=sys.stderr);

  cpus = get_num_cpus()-1;

  pool = AsyncPool(cpus);


  # NOTE: this is kind of annoying. The unittest module can't really
  # deal with the multiple processes. You need control on entry and
  # exit of the children and it just isn't setup for that. So, we
  # try to follow the unittest pattern more or less and do something very
  # similar here in the event it is ever possible to refactor to use
  # unittest or some future unittest module.

  class TestBsync() :
    def assertTrue(self,arg) :
      assert(arg);
    def assertFalse(self,arg) :
      assert(not arg);
    def test_simple(self) :
      task = pool.async(myhellfunc,"Jolene");
      s = task.get();
      self.assertTrue(s == myhellfunc("Jolene"));
    def test_random(self) :
      tasks = [pool.async(myrandfunc,0,10,i) for i in range(10)];
      for n,t in enumerate(tasks) :
        x = t.get();
        self.assertTrue(x.shape == (n,n));
    def test_timeout(self) :
      task = pool.async(mytimeout,0.1,timeout=1);
      self.assertTrue(task.get() == True);
      try :
        task = pool.async(mytimeout,2.,timeout=1);
        self.assertTrue(task.get() == True);
      except Exception as e :
        self.assertTrue(isinstance(e,OSError));
    def test_large(self) :
      x = [_.get() for _ in [pool.async(myrandfunc,0,10,5) for i in range(1000)]];
      return True;
    def test_strfunc(self) :
      task = pool.async("myhellfunc","Jolene");
      s = task.get();
      self.assertTrue(s == myhellfunc("Jolene"));
    def test_get_cpus(self) :
      fprint("%d cpus..." % get_num_cpus(),end='');
    def test_multiple_pools(self) :
      """ test having multiple pools open at one time """
      if not use_mpi or not cpus is None :
        pool2 = AsyncPool(1);
        jobs = [_.get() for _ in [pool2.async(myhellfunc,j) for j in range(5)]];
        del pool2;
  
  obj = TestBsync();
  success = fails = 0;
  for nm,f in TestBsync.__dict__.items() :
    if nm.startswith("test_") :
      fprint("Testing %s..." % nm,end='');
      try :
        f(obj);
        fprint("OK");
        success += 1;
      except Exception as e :
        fprint("Failed!\n",str(e));
        fails += 1;
  fprint("%d tests succeeded, %d failures" % (success,fails))
  fprint("Using MPI with %d+1 ranks" % cpus \
      if use_mpi else "Using subprocess fork, not MPI");

  del pool;

