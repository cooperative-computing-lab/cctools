# Flow Manager
-------------
A management system that monitors an input directory and runs the specified workflow on the input files.
It then outputs the result to an outbox.

## Usage
---------
Create an inbox, outbox, and error directory.
Then run 
```
python main.py -i inbox -o outbox -m /path/to/makeflow/file -e error
```
This will monitor start monitoring the inbox directory for input files. 

## Configuring your makeflow file
----------------------------------
- The makeflow file must be in the same directory as the code necessary to run it. This is because the entire
directory containing the makeflow file is copied before being run.
- The primary datafile or input file in the makeflow file should be `input.tar.gz`. It is your responsible to
unpack that file in your makeflow file.
- All desired output files must be packed into `output.tar.gz`. This is the file put in the outbox or error directory in the event of an error.

## Additional Options:
-----------------------

--process-limit (-p) [num] specifies a max number of workflows to run at once (default=5)

