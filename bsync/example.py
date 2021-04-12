#!/usr/bin/env python3

from bsync import *
from time import sleep

def myfunc(x) :
#  print("@ myfunc got",x);
#  raise Exception("hell world!");
  return x+2;

with AsyncPool(4) as mypool :
  ms = [mypool.async(myfunc,i) for i in range(20)];
  # optional test ready
  while not all([_.ready() for _ in ms]) : sleep(0.1);
  # blocks if not ready
  print([_.get() for _ in ms]);

