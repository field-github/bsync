#!/usr/bin/env python3

""" A simple example program for bsync to demonstrate having two
  MPI threadpools open at the same time. Note that the tag
  for communications needs to be specified and must be different
  for the two separate pools. In this case, we manually arrange
  for the deletion of the pools at the end of the program.
"""

from bsync import *
from time import sleep
from os import getpid
from random import *

def myfunc(x,pn) :
  """ return a sample test string
    Args:
      x(Any):     any argument to be returned in string
      sz(int):    number of tasks/ranks in AsyncPool
    Returns:
      str:        a test string
  """
  sleep(random());
  s = "bsync testing "+str(x);
  if bsync_using_mpi() :
    s += "  from rank %d" % bsync_get_rank();
  else :
    s += "  from pid %d" % getpid();
  s += " out of pool # %d" % pn;
  return s;

# create the two MPI threadpools
pool1 = AsyncPool([1,2,3],tag=100);
pool2 = AsyncPool([3,],tag=101);

# quaue up some jobs in both pools
jobs1 = [pool1.async(myfunc,i,1) for i in range(5)];
jobs2 = [pool1.async(myfunc,i,2) for i in range(5)];

# wait for everybody to get donw
while not all([_.ready() for _ in jobs1]) and \
      not all([_.ready() for _ in jobs1]) : sleep(0.01);

# and print the results (in the original queueing order)
print("\n".join([_.get() for _ in jobs1+jobs2]));

# delete the two threadpools
del pool1
del pool2

