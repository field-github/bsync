#!/usr/bin/env python3

""" multi.py
a program that runs a script with arguments in parallel using MPI
usage:

  srun -N 4 --ntasks-per-node=12 -p pdebug multi.py scriptname [-allopts options for all --] args...

  (if scriptname is '-', the script will be taken from stdin and created in a temporary file)

  the script will be spawned once for each argument in the args... list
  as in
  scriptname [options for all] arg0
  scriptname [options for all] arg1
  scriptname [options for all] arg2
  ...

  extra options :
    -multi_file filename.txt      <- add lines from filename.txt to args... (can use multiple files)
    -multi_split n                <- split the args into n groups and feed those groups to the program
    -multi_break sz               <- split the args in to groups of size n

  So, these options can be used to more flexibly break up the job into pieces if the
  target execution script can handle the parameters

  A typical way to use this to launch yorick scripts would be
  srun -N 4 --ntasks-per-node=12 -p pdebug "yorick -batch myscript.i" myarg0 myarg1 ...
  This will launch the yorick script myscript.i repeatedly using the arguments
  myarg0, myarg1, ... in sequence, but the execution will happen in parallel
  to the limit of the requested size of the MPI job

  Extended argument syntax with substitution :
    You can use $1 $2, etc. in an -allopts section to substitute values from the args.
    In this case, args is split by commas in order to find the subsitution values.
    e.g.  multi.py /bin/echo --allopts $1 $2 -- 1,2 3,4 5,6
    would print to the screen
    1 2
    3 4
    5 6
    Note that in this case, no argument will be appended to the end of the arguments
    to the comand line, e.g. only $n substitutions will take place
    You can also use $n as a substitution for the sequence number in multi, starting
    from 0. This is useful to direct output to a collection of different files.

  Expansion of numerical count :
    You can add any argument to multi with bracket syntax to repeat with a
    count. For example :
      multi.py /bin/bash --allopts -c -- myscript[1-400].sh   # run myscript1.sh ... myscript400.sh
        OR 
      multi.py /bin/bash --allopts -c -- \"echo [1-400]\"     # echo 1-400
    This would run 'bash -c myscript1.sh' ...  'bash -c myscript400.sh'.  So,
    the numerical expansion is *inclusive* of the range provided.

"""

import sys
from bsync import AsyncPool
from os.path import exists
from os import unlink,getpid,chmod
from time import sleep
import stat
from itertools import chain
from re import search,sub

if "--help" in sys.argv or "-h" in sys.argv :		# check for help request in arguments
  print(__doc__);		# if so, print docstring
  exit(1);

def expand_arg(a) :
  x = search(r"(\[[0-9\-]+\])",a);
  if not x : return [a,];
  x = x.group(1);   # extract the pattern
  try :
    lo,hi = [int(_) for _ in search(r"(\d+)\-(\d+)",x).group(1,2)];
  except Exception as e :
    println("need to have correct format for argument : " + a);
    raise e;
  return [a.replace(x,str(i)) for i in range(lo,hi+1)];

task_name = sys.argv[1];	# get the name of the script to run
dele_task = False;
if task_name == '-' :		# if stdin is the task name, create tempfile
  task_name = "tempfile_multi_%d.exe" % getpid();
  with open(task_name,"w") as f : f.write(sys.stdin.read());
  chmod(task_name,stat.S_IRUSR|stat.S_IXUSR|stat.S_IWUSR);
  dele_task = True;

# deal with multi_file option
while "-multi_file" in sys.argv :
  idx = sys.argv.index("-multi_file");
  sys.argv.pop(idx);
  sys.argv += map(str.strip,open(sys.argv.pop(idx)).readlines());    # add to arglist

if "--allopts" in sys.argv :
  sys.argv[sys.argv.index("--allopts")] = "-allopts";   # replace --allopts

if "-allopts" in sys.argv :
  id0 = sys.argv.index("-allopts");
  id1 = sys.argv.index("--");
  opts = " ".join(sys.argv[id0+1:id1])+" ";
  sys.argv = sys.argv[0:id0]+sys.argv[id1+1:];
else : opts = " ";

if "-multi_break" in sys.argv :
  idx = sys.argv.index("-multi_break");
  sys.argv.pop(idx);
  sz = int(sys.argv.pop(idx));
  nargs = len(sys.argv)-2;
  sys.argv = sys.argv[0:2]+[" ".join(sys.argv[i+2:i+sz+2]) for i in range(0,nargs,sz)];

if "-multi_split" in sys.argv :
  idx = sys.argv.index("-multi_split");
  sys.argv.pop(idx);
  n = int(sys.argv.pop(idx));
  nargs = len(sys.argv)-2;
  sys.argv = sys.argv[0:2]+[" ".join(sys.argv[i*nargs/n+2:(i+1)*nargs/n+2]) for i in range(0,n)];

sys.argv = list(filter(lambda s:s.strip(),sys.argv));   # remove empty entries
# expand [1-10] syntax
sys.argv = sys.argv[:1] + list(chain(*map(expand_arg,sys.argv[1:])));

def launch(tname,opts,arg,seqno) :
  """ function to launch a script with argument arg """
  from os import system		# must import system in MPI context
  clear_arg = False;
  if search(r"\$n",opts) :        # extended opts are chosen
    while True :
      x = search(r"\$n",opts);
      if x :
        opts = opts.replace(x.group(0),str(seqno));
      else : break;
    clear_arg = True;
  if search(r"\$\d+",opts) :        # extended opts are chosen
    l = arg.split(",");
    while True :
      x = search(r"\$(\d+)",opts);
      if not x : break;
      opts = opts.replace(x.group(0),l[int(x.group(1))-1]);
    clear_arg = True;
  if clear_arg : arg = "";
  cmdstr = tname+" "+opts+arg;	# run the script with argument
  q = system(cmdstr);	# run the script with argument
  if q :
    raise Exception("multi.launch: problem (%s) running script '%s'" % (str(q),cmdstr));

###########################################################################
# create a pool of tasks to call launch with args task_name and argn
# and then map .get() on the task pool to wait for each task to finish up
# Note that if you want finer control of the process configuration/MPI ranks
# there are arguments to AsyncPool that will allow you to do that.
# Also note that the pool object *must* be deleted before the program exits
# otherwise the slave processes will hang waiting for the message to exit.
###########################################################################
pool = AsyncPool();

tasks = [pool.bsync(launch,task_name,opts,i,seqno) for seqno,i in enumerate(sys.argv[2:])];

while tasks :
  q = [_ for _ in tasks if _.ready()];
  if q :
    for i in q :
      tasks.remove(i);
      i.get();
  sleep(0.5);

del pool;			# delete the pool when done

if dele_task :			# delete the task file if it was a temporary
  unlink(task_name);

