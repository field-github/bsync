#!/usr/bin/env python3

from bsync import *
from time import sleep
from os import getpid

def myfunc(x,sz) :
  """ return a sample test string
    Args:
      x(Any):     any argument to be returned in string
      sz(int):    number of tasks/ranks in AsyncPool
    Returns:
      str:        a test string
  """
  s = "bsync testing "+str(x);
  if bsync_using_mpi() :
    s += "  from rank %d" % bsync_get_rank();
  else :
    s += "  from pid %d" % getpid();
  s += " out of %d tasks" % sz;
  return s;

with AsyncPool() as mypool :
  ms = [mypool.async(myfunc,i,mypool.get_size()) for i in range(10)];
  # optional test ready
  while not all([_.ready() for _ in ms]) : sleep(0.1);
  # blocks at each task if not ready
  for i in ms :
    print(i.get());
  # alternative is [_.get() for _ in ms] to capture all

