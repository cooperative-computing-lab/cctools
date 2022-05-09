# Flow Manager
-------------
A management system that monitors an input directory and runs the specified workflow on the input files.
It then outputs the result to an outbox.

## Usage
---------
Create an inbox, outbox, and error directory.
Then run 
```
python main.py -i /path/to/inbox -o /path/to/outbox -m /path/to/makeflow/file -e /path/to/error
```
This will monitor start monitoring the inbox directory for input files. 
When they are processed, they will be moved to the outbox directory if successful, if not, then they are moved to the error directory.

## Configuring your makeflow file
----------------------------------
- The makeflow file must be in the same directory as the code necessary to run it. This is because the entire
directory containing the makeflow file is copied before being run.
- The primary datafile or input file in the makeflow file should be `input.tar.gz`. It is your responsible to
unpack that file in your makeflow file.
- All desired output files must be packed into `output.tar.gz`. This is the file put in the outbox or error directory in the event of an error.


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
	python main.py -i inbox -o outbox -m examples/pairs_combo/pairs.mf -e error
```
	3. Add a bunch of input files to the inbox directory
```
	python tools/run_experiment_combo.py 40 inbox
```
Now Mufasa should be running and begin processing the input files.

## Improvements
---------------
- Add a way to dynamically adjust the default expected resource consumption of a workflow
- Detect if the global resources are insufficient for a workflow to ever be completed
- Monitor the read/write bandwidth on the shared filesystem
- Improve the scheduling and allocation policy to be more intelligent and attempt to reduce fragmentation
- Add a way to dynamically adjust a WMS's resource limits without pausing or killing the WMS

