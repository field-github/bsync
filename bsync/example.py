#!/usr/bin/env python3

""" simple example program for bsync showing how to use the MPI
    threadpool. In this case, we wrap the threadpool using a 'with'
    block, but you can also arrange to delete the threadpool
    manually. See README.md
"""

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


# create the threadpool with as many cpus as are available
with AsyncPool() as mypool :
  tasks = [mypool.async(myfunc,i,mypool.get_size()) for i in range(10)];
  # optional test for all tasks complete
  while not all([_.ready() for _ in tasks]) : sleep(0.1);
  # since we've tested for all ready, this loop will not block.
  # if the polling loop above is removed, then this print statement
  # will block one by one until each task is completed
  for i in tasks :
    print(i.get());

  # an alternative for acquiring all the tasks is to use a
  # comprehension like [_.get() for _ in ms]

# deletion of threadpool is automatic here in 'with' block.

