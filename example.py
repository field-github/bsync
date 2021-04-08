#!/usr/bin/env python3

from bsync import *
from time import sleep

def myfunc(x) :
  print("@ myfunc got",x);
#  raise Exception("hell world!");
  return x+2;

with AsyncPool(3) as mypool :
  if True :
    m1 = mypool.async(myfunc,4,timeout=1);
    m2 = mypool.async(myfunc,5);
    while not m1.ready() or not m2.ready() : sleep(0.01);
    print("@@ m1 ",m1.get(),m2.get());

  ms = [mypool.async(myfunc,i) for i in range(200)];
  while not all([_.ready() for _ in ms]) : sleep(0.1);
  for i in ms : print(i,i.get());


