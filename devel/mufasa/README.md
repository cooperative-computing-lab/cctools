# Mufasa
-------------
A workflow ensemble is a set of workflows that need to be processed by a research campaign.
Mufasa is a tool for managing the ensemble given a set of global resource limits for cores usage, memory usage, number of jobs, and disk consumption.
Mufasa will schedule and monitor WMSs such that the total resource consumption does not exceed these global limits.
The current iteration of Mufasa can run and monitor Makeflows, although this will someday be expanded to include a wider variety of WMSs.

## Configuring your Makeflow file
----------------------------------
- The makeflow file must be in the same directory as the code necessary to run it. This is because the entire
directory containing the makeflow file is copied before being run.
- The primary datafile or input file in the makeflow file should be `input.tar.gz`. It is your responsible to
unpack that file in your makeflow file.
- All desired output files must be packed into `output.tar.gz`. This is the file put in the outbox or error directory in the event of an error.
- In order for Mufasa to properly manage disk consumption, the Makeflow should specify the size of files between tasks.

## Basic Usage
---------
The general structure for configuring Mufasa is shown in the src/mufasa.py file.
The point of this file is to create a program that can be run like the following:
```
python src/mufasa.py -i /path/to/inbox -o /path/to/outbox -m /path/to/makeflow/file -e /path/to/error
```
This will monitor start monitoring the inbox directory for input files. 
When they are processed, they will be moved to the outbox directory if successful, if not, then they are moved to the error directory.

***IMPORTANT NOTE***: You should ***NOT*** `cp` files into the inbox directory. Instead, you must `mv` them. The reason for this is because `cp` is not an atomic operation, thus the file is created before the data is copied into it. This means it is possible for Mufasa to begin processing the file before it is fully copied. The solution is to just `mv` the file since this is an atomic operation.

This program performs the following basic operations. 
The first step is to create an instance of the Workflow() class.
```
wf = Workflow(path_to_makeflow_file, name_of_expected_inputfile)
```
The `path_to_makeflow_file` is the absolute path to the makefile which must be in the same directory as the source code necessary for it to run.
The expected input file should be the name of the tarfile that makefile unpacks to extract the other input files.

Then you must create an instance of a WorkflowScheduler.
The first three arguments of a WorkflowScheduler specify the path to the outbox directory, the path to the created Workflow, and the path to the error directory.
The two other options are `total_limits` and `workflow_limits` which both have default values specified in the WorkflowScheduler.py module, but it is highly recommended to override them.
The total resource limits can be specified as a dictionary and should be passed as an argument to the WorkflowScheduler(). 
They must be a dictionary similar to the following:
```
total_limits = {
	"cores": 800,
	"memory": 2000,
	"jobs": 1000,
	"disk": 75000
}
```

The estimated default limits for workflows should also be a dictionary passed as an argument to the WorkflowScheduler().
```
workflow_limits = {
	"cores": 100,
	"memory": 400,
	"jobs": 200,
	"disk": 5000
}
```

***IMPORTANT***: The units for cores are core usage percentage. Thus, 100 signifies 1 core, 200 signifies 2 cores, etc. This enables greater flexibility in regulating resources than specifying a whole number of cores. The units for memory and disk are both MB.

Thus, if you wanted to create a WorkflowScheduler with the above limits you would run:
```
scheduler = WorkflowScheduler(path_to_outbox, instance_of_workflow, path_to_error_dir, total_limits=total_limits, workflow_limits=workflow_limits)
```

Next, you must create a DirectoryMonitor to monitor the inbox directory for new files.
```
dm = DirectoryMonitor(path_to_inbox, scheduler)
```
To check the directory for new files:
```
dm.monitor()
```
This compares the current state of the directory with the previous state, and pushes any new files to the scheduler's internal queue.
Then to schedule WMSs run:
```
scheduler.schedule()
```
which checks the internal scheduling queue for inputs and starts a WMS if the `workflow_limits` fit within the `total_limits`.

To combine these two commands to routinely monitor a directory and then schedule workflows, you can create a loop like this:
```
while True:
	scheduler.schedule()
	dm.monitor()
```

## Running an example
----------------------
The examples directory contains a few different types of workflows.
The `examples/pairs_combo/` is probably the best example to run.
To run an instance of this example workflow, perform the following:
1. Create the inbox, outbox, error directories
``` 
mkdir inbox
mkdir outbox
mkdir error
```
2. Start mufasa
```
python src/mufasa.py -i inbox -o outbox -m examples/pairs_combo/pairs.mf -e error
```
3. Add a bunch of input files to the inbox directory
```
python tools/run_experiment_combo.py 40 inbox
```
Now Mufasa should be running and begin processing the input files.


## Possible Improvements
---------------
- Add a way to dynamically adjust the default expected resource consumption of a workflow (modify WorkflowScheduler.py)
- Detect if the global resources are insufficient for a workflow to ever be completed (WorkflowScheduler.py)
- Monitor the read/write bandwidth on the shared filesystem (WorkflowProfiler.py, Workflow.py, WorkflowScheduler.py)
- Improve the scheduling and allocation policy to be more intelligent and attempt to reduce fragmentation (WorkflowScheduler.py
- Add a way to dynamically adjust a WMS's resource limits without pausing or killing the WMS (Workflow.py, WorkflowScheduler.py)

