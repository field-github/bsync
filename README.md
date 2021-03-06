# bsync

A threadpool library for MPI tasks. It is a python3 compatible and enhanced version of an
earlier library named async. It uses mpi4py or mpi4pylite as the underlying communication
engine. Note that all commands and results **must be pickleable** to be sent through the
stream. (see Gotchas below)

## INSTALL

The normal thing:

	python3 setup.py build
	python3 setup.py install

## FEATURES

Works with MPI or subprocess fork tasks when MPI is not available. Timeouts
and task priorities are new features. AsyncPool class now has __enter__ and __exit__
properties. Communication tags can be specified for MPI. Multiple threadpools can be
active at one time and it is possible to make mixed threadpools that have *both* MPI and
forked subprocesses. The individual MPI ranks can be specified and not all ranks
need to be used for the task execution. Tags can be specified for the communications
so that backchannel communications can be used with different tags.

## SYNOPSIS

Instantiate your AsyncPool object with the number of desired tasks. This may be less than
the number of total MPI tasks in your job. The AsyncPool object needs to be deleted when
you're done with it. This happens automatically in the `with` block. The following code
snippet will remotely call the function `myfunc` on 4 MPI ranks (using the MPI communicator
tag 1234) and pass the function `myfunc` an integer argument from 0..19.

	from bsync import *
	with AsyncPool(4,[tag=1234]) as mypool :
	  ms = [mypool.bsync(myfunc,i) for i in range(20)];
	  print([_.get() for _ in ms]);

The `bsync` method can take a variable arglist of position arguments. Because of the
problem of collision in keyword arguments between the `bsync` method and the function to
be called, if you need to pass keyword args just write a wrapper function to accept a dict
of keywords and then call the underlying function from it, e.g. :

	def myfunc(x,y,mykeywordarg="blah blah") : ...

	def myfunc_wrapper(*v,args_dict) :
	  return myfunc(*v,**args_dict);

An alternative to managing the scope of the AsyncPool using `with` blocks is to do the
deletion manually :

	from bsync import *
	mypool = AsyncPool(4);
	ms = [mypool.bsync(myfunc,i) for i in range(20)];
	print([_.get() for _ in ms]);
	del mypool;

The `AsyncPool.bsync` method returns a `MessageHandle` object which has both `get` and
`ready` methods. The `get` method blocks until the gotten task returns its result. The
`ready` method is a non-blocking poll to test whether a result has been returned.
	
	with AsyncPool(4) as mypool :
	  ms = [mypool.bsync(myfunc,i) for i in range(20)];
	  # poll until all jobs ready
	  while not all([_.ready() for _ in ms]) : sleep(1);
	  print([_.get() for _ in ms]);		# will not block here

Timeouts and priority can be added to the task request created by the `bsync` method :

	job = mypool.bsync(myfunc,i,timeout=3,priority=-2)

In this case, there will be a 3 second timeout for the function to complete. The default
priority is zero; more negative priority is **higher** priority. So, if many tasks are
alrady queued at say zero priority, you can queue up a task with -1 priority and it will
be the next one submitted for execution (e.g. as soon as a MPI task becomes available.)

## EXCEPTIONS

Exceptions that occur in the remote processes are returned through the MPI communication
and will by default raise in the master process :

	job = mypool.bsync(myfunc,"hello world!");
	try :
	  result = job.get();
	except Exception as e :
	  print("Ooops! myfunc caused an exception %s" % str(e));

This behavior can be changed by setting the `reraise=False` keyword arg in the `get`
method in which case the return value will be a ProcException whose property `e.exc` will
be the exception that was actually thrown.  Note that the `ready` method will not raise an
exception coming from the underlying polled task. That only happens from the `get` method
and so it is not necessary to wrap the `ready` test inside of a `try...catch` block

## OPERATION WITH/WITHOUT MPI

When MPI is not available, **bsync** will default back to subprocess fork mode in which
the communications are handled by Unix pipes. When MPI is available, if no number of tasks
is provided to the AsyncPool constructor, it will default to the size of the MPI job minus
one (rank 0 used by the controlling process). For subprocess, it will default to the
number of CPUs on the local machine. When in MPI, it is also possible to specify a list of
ranks to the AsyncPool constructor and then only use those ranks for the task execution.
This makes is possible to do other MPI operations from the executing subtasks on the ranks
which are not being used in the AsyncPool task pool, e.g.

	mypool = AsyncPool([1,2,3,4,5]);		# only use these 5 ranks
							# for task execution

Even when in MPI mode, you can force **bsync** to use subprocess fork by setting the
variable `bsync_use_mpi` to False :

	import __main__
	__main__.bsync_use_mpi = False

### MIXED MPI/FORK PROCESS POOLS

It is possible to mix tasks between MPI and forked subprocesses. See the example program
`example_mixed.py`.  This is done by specifying a list for the AsyncPool constructor.
Elements of the list are either integer rank numbers or `None` for a requested forked
subprocess, e.g.

	# use ranks 1,2,3 and three forked subprocesses
	with AsyncPool([None,1,2,3,None,None]) as mypool :
	  ...

No distinction is made between the subprocesses in terms of where tasks will be
submitted.

### EXAMPLES

There are three simple example programs. The second one, `example_twin_pools.py` demonstrates
the use of two separate threadpools at the same time.

### MISCELLANEOUS

`AsyncPool.get_size()` returns the number of tasks in the thread pool.
`bsync_using_mpi()` returns True if the threadpool is operating in MPI mode.
`bsync_get_rank()` returns the MPI rank number for this task when called in MPI
mode. Note that this works either in a task function or in the main controller
task.

### TESTING

There is a unittest which can be executed by treating the module as a standalone
executable, e.g. just type `./bsync.py` or `mpirun -np 4 ./bsync.py`

### GOTCHA

Python has limitations on the ability to pickle function objects. You may have functions
which are not pickleable (e.g. contain unpickleable code) and for this, you can substitute
the raw function object in bsync for the name of the function, e.g.

	task = mypool.bsync("myfunc",i);

instead of 

	task = mypool.bsync(myfunc,i);

when the task controller receives a string instead of a function object, it will eval the
string and then attempt to call the (hopefully) resulting function object.

