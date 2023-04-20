# Filetrace

## Overview

Filetrace is a command line tool that summarizes the output of the linux debug tool [strace](https://github.com/strace/strace) to help users view what files a program accesses and better understand a program's dependencies. It is made to run on a range of applications from simple to larger, distributed applications.

## Usage

filetrace can be called directly from the command line using:

```sh
$ filetrace <command_to_excecute>
```

filetrace also has optional command line flags:

* `-d <num>`  :  minimum level to combine directories to make the summary for output 3
* `-t <num>`: only show the top \<num> of results for output 3
* `-j`: json mode
* `-a`: assemble mode
* `-n`: filename prefix
* `--clean`: remove all filetrace files in the current directory

#### Note

Due to `strace` being a linux only tool, filetrace currently only supports linux.

### Output files

Filetrace will create several files, depending on its mode.

In standard and assemble mode it creates four files:

| File                     | Description                                                  |
| ------------------------ | ------------------------------------------------------------ |
| `<name>.filetrace-1.txt` | The output of `strace`, the most verbose                     |
| `<name>.filetrace-2.txt` | A list of all the files accessed by the program with the number of times it was accessed and what action was performed |
| `<name>.filetrace-3.txt` | A summary of the actions performed, listed by directory      |
| `<name>.filetrace-4.txt` | A list of the files associated with the pid of the any subprocesses called |

In json mode filetrace creates two json files:

| File                            | Description                               |
| ------------------------------- | ----------------------------------------- |
| `filetrace-process-<name>.json` | PIDs and the command associated with them |
| `filetrace-path-<name>.json`    | each file path with its attributes        |

#### Action Legend

| Key  | Meaning           | Description                                                  |
| ---- | ----------------- | ------------------------------------------------------------ |
| R    | Read              | file was opened with read only access                        |
| W    | Write             | file was opened with write only access                       |
| WR   | Write/Read        | file was opened with both read and write access              |
| OU   | Open Unsuccessful | there was an attempt to open the file but the file was not found |
| S    | Stat              | stat was called on the file                                  |
| SU   | Stat Unsuccessful | stat was called on the file but the file was not found       |
| M    | Memory Map        | File was opened and only `mmap` was called                   |
| ?    | Unknown           | File was found in `strace` output but filetrace could not determine it's action |



## Usage on Batch Systems

Filetrace comes with two modes to help analyze files on batch systems or distributed applications.

Json mode, activated with the `-j` flag, tells filetrace to output 2 json files, rather than its normal summary files. This is useful for running on each worker.

Assemble mode, activated with the `-a` flag, creates a summary using all the filetrace json files in the current directory. This is run after all the workers have finished their tasks.

#### Naming Files

Many batch systems required specifying output files when submitting a job. By default filetrace names its files using the word immediately following the filetrace command, which is typically the name of the application being traced. However, this can create naming conflicts when multiple jobs are running the same application.

This is solved using  the `-n` name flag which allows the user to specify the prefix of the output file. This can be tied to something like the task ID of the job so each job has a unique filename.

## Usage

### Using filetrace on `touch`

filetrace can be run simply by typing `filetrace` before the command you would like to execute.

To see all the files `touch` accesses when creating a blank file named "this" we can use:

```sh
$ filetrace touch this

filetrace: syscalls processed: 43
----- filetrace -----
filetrace completed

Created summaries:
touch.filetrace-1.txt : output of strace
touch.filetrace-2.txt : the action and frequency performed on each file
touch.filetrace-3.txt : summary of all the actions
touch.filetrace-4.txt : summary of files accessed by subprocesses
```

As we can see, filetrace created 4 different summary files:

touch.filetrace-1.txt:

```
execve("/usr/bin/touch", ["touch", "this"], 0x7ffcb6b08210 /* 55 vars */) = 0
access("/etc/ld.so.preload", R_OK)      = -1 ENOENT (No such file or directory)
openat(AT_FDCWD, "/afs/crc.nd.edu/group/ccl/software/x86_64/redhat8/condor/current/lib/glibc-hwcaps/x86-64-v2/libc.so.6", O_RDONLY|O_CLOEXEC) = -1 ENOENT (No such file or directory)
...
```
touch.filetrace-2.txt
```
   W       0   1  /afs/crc.nd.edu/user/j/jdolak/ccl/cctools/filetrace/this
  SU       0   2  /afs/crc.nd.edu/user/c/condor/software/versions/x86_64-redhat8/condor-8.8.14/lib/tls/x86_64
  SU       0   2  /afs/crc.nd.edu/user/c/condor/software/versions/x86_64-redhat8/condor-8.8.14/lib/x86_64
	...
```
touch.filetrace-3.txt
```
Major Directories
     864 2      /usr/lib64/
       0 18     /afs/crc.nd.edu/user/c/condor/software/versions/x86_64-redhat8/condor-8.8.14/lib
       0 10     /afs/crc.nd.edu/user/j/jdolak/ccl/cctools/filetrace
       ...
```
touch.filetrace-4.txt

* touch.filetrace-4.txt is empty because touch did not call any forks 

```
Subproccesses: 

```

### Using filetrace on larger applications

filetrace can be run on larger applications such as a HECIL genomics analysis workflow from [cooperative-computing-lab](https://github.com/cooperative-computing-lab)/**[makeflow-examples](https://github.com/cooperative-computing-lab/makeflow-examples)**

```sh
$ filetrace makeflow hecil.mf

----- filetrace -----
filetrace completed

Created summaries:
makeflow.filetrace-1.txt : output of strace
makeflow.filetrace-2.txt : the action and frequency performed on each file
makeflow.filetrace-3.txt : summary of all the actions
makeflow.filetrace-4.txt : summary of files accessed by subprocesses
```

makeflow.filetrace-2.txt:

```
action bytes freq path
  WR  16.33G 3920   /afs/crc.nd.edu/user/j/jdolak/ccl/makeflow-examples/hecil/ref.fasta.bwt
  WR   6.99G 1764   /afs/crc.nd.edu/user/j/jdolak/ccl/makeflow-examples/hecil/ref.fasta.sa
  WR   2.15G 512    /afs/crc.nd.edu/user/j/jdolak/ccl/makeflow-examples/hecil/ref.fasta.pac
  WR   1.68G 230799  /afs/crc.nd.edu/user/j/jdolak/ccl/makeflow-examples/hecil/pileup.txt
  ...
```

Here we can also see the different subprocesses makeflow called:

makeflow.filetrace-4.txt:

```
Subproccesses: 

pid : 2623721 : "python3 ./fasta_reduce ref.fasta 1000"
  2623721   S   /afs/crc.nd.edu/user/c/condor/software/versions/x86_64-redhat8/condor-8.8.14/lib/x86_64/x86_64
  2623721   WR  /afs/crc.nd.edu/user/j/jdolak/ccl/makeflow-examples/hecil/ref.fasta.78
  2623721   S   /afs/crc.nd.edu/user/j/jdolak/miniconda3/envs/cctools-env/bin/pyvenv.cfg
  2623721   WR  /afs/crc.nd.edu/user/j/jdolak/ccl/makeflow-examples/hecil/ref.fasta.20
 ...
```

### Using filetrace with distributed applications

#### Filetrace with Work Queue

Using the gzip example from the [Work Queue documentation](https://cctools.readthedocs.io/en/latest/work_queue/) we can run filetrace while using Work Queue just by adding the following lines to the existing python script:

```
ftrace_outfile1 = "filetrace-path-%d.json" % (i)
ftrace_outfile2 = "filetrace-process-%d.json" % (i)

command = "./filetrace -n %d -j ./gzip < %s > %s" % (i,infile, outfile)
t.specify_file("./filetrace.py", "filetrace", WORK_QUEUE_INPUT, cache=True)
t.specify_file(ftrace_outfile1, ftrace_outfile1, WORK_QUEUE_OUTPUT, cache=False)
t.specify_file(ftrace_outfile2, ftrace_outfile2, WORK_QUEUE_OUTPUT, cache=False)
```

Filetrace will output two files for each task. With the `-n` flag we have specified the name output files to correspond to the ID of the task.

```
$ ./manager-filetrace-gzip.py a b

listening on port 9123...
submitted task (id# 1): ./filetrace -n 1 -j ./gzip < a > a.gz
submitted task (id# 2): ./filetrace -n 2 -j ./gzip < b > b.gz
waiting for tasks to complete...
task (id# 2) complete: ./filetrace -n 2 -j ./gzip < b > b.gz (return code 0)
task (id# 1) complete: ./filetrace -n 1 -j ./gzip < a > a.gz (return code 0)
all tasks complete!
```

After completion, this gives us the files:

```
filetrace-path-1.json
filetrace-path-2.json
filetrace-process-1.json
filetrace-process-2.json
```

Running `filetrace -a gzip` assembles the output files into summaries with the prefix "gzip".

```
$ filetrace -a gzip

----- filetrace -----
filetrace completed

Created summaries:
gzip.filetrace-2.txt : the action and frequency performed on each file
gzip.filetrace-3.txt : summary of all the actions
gzip.filetrace-4.txt : summary of files accessed by subprocesses
```

gzip.filetrace-2.txt :

```
action bytes freq path
   W      20 1      /tmp/worker-241296-2791364/t.1/a.gz
   W      20 1      /tmp/worker-241296-2791364/t.2/b.gz
   ...
```


