#!/usr/bin/env python3

""" simple example program for bsync showing how to mix MPI and
    forked subprocesses.
"""

from bsync import *
from os import getpid

def myfunc(x) :
  global mpi_rank;
  try :
    myrank = mpi_rank;
  except : myrank = -1;
  return "Hello World "+str(x)+" from rank %d and pid %d" % (myrank,getpid());

# create the threadpool with 3 subprocesses and ranks 1,2,3
with AsyncPool([None,1,2,3,None,None]) as mypool :
  tasks = [mypool.async(myfunc,i) for i in range(10)];
  for i in tasks :
    print(i.get());


