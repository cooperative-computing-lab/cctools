## Running a TaskVine Application

There are a variety of ways to execute a TaskVine application at scale.
The examples in this section make use of the example program
[functions.py](examples/functions.py)
which you can download to execute like this:

```
python3 functions.py
```

### Language Specific Setup

Before running the application, you may need some
additional setup, depending on the language in use:

#### Python Setup

If you installed via conda, then no further setup is needed.

If you are running a Python application and did *not* install via conda,
then you will need to set the `PYTHONPATH` to point to the cctools
installation, like this:

```sh
# Note: This is only needed if not using conda:
$ PYVER=$(python -c 'import sys; print("%s.%s" % sys.version_info[:2])')
$ export PYTHONPATH=${HOME}/cctools/lib/python${PYVER}/site-packages:${PYTHONPATH}
```

#### C Language Setup

If you are writing a TaskVine application in C, you should compile it into an executable with a command like this. Note that this example assumes that CCTools has
been installed using the `conda` method.

```sh
gcc taskvine_example.c -o taskvine_example -I${CONDA_PREFIX}/include/cctools -L${CONDA_PREFIX}/lib -ltaskvine -ldttools -lm -lz -lssl -lcrypto
```

### Running a Manager Program

The example application simply compresses a bunch of files in parallel. The
files to be compressed must be listed on the command line. Each will be
transmitted to a remote worker, compressed, and then sent back to the
manager. To compress files `a`, `b`, and `c` with this example
application, run it as:


```sh
# Python:
$ ./taskvine_example.py a b c

# C
$ ./taskvine_example a b c
```

You will see this right away:


```sh
listening on port 9123...
submitted task: /usr/bin/gzip < a > a.gz
submitted task: /usr/bin/gzip < b > b.gz
submitted task: /usr/bin/gzip < c > c.gz
waiting for tasks to complete...
```

The TaskVine manager is now waiting for workers to connect and begin
requesting work. (Without any workers, it will wait forever.) You can start
one worker on the same machine by opening a new shell and running:


```sh
# Substitute the IP or name of your machine for MACHINENAME.
$ vine_worker MACHINENAME 9123
```

If you have access to other machines, you can simply `ssh` there and run workers as well. In general, the more workers you start, the faster the work gets done. If a
worker fails, the TaskVine infrastructure will retry the work
elsewhere, so it is safe to submit many workers to an unreliable system.

### Submitting Workers to a Batch System

If you have access to a HTCondor pool, you can use this shortcut to submit ten
workers at once via HTCondor:

```sh
$ vine_submit_workers -T condor MACHINENAME 9123 10

Submitting job(s)..........
Logging submit event(s)..........
10 job(s) submitted to cluster 298.
```

This will cause HTCondor to schedule worker jobs on remote machines.
When they begin to run, they will call home to the indicated machine
and port number, and begin to service the manager application.

Similar scripts are available for other common batch systems:

```sh
$ vine_submit_workers -T slurm MACHINENAME 9123 10
$ vine_submit_workers _T uge MACHINENAME 9123 10
```

When the manager completes, if the workers were not otherwise shut down,
they will still be available, so you can either run another manager
with the same workers, or you can remove the workers with `kill`, `condor_rm`,
or `qdel` as appropriate. If you forget to remove them, they will exit
automatically after fifteen minutes. (This can be adjusted with the `-t`
option to `worker`.)

### Project Names and the Catalog Server

Keeping track of the manager's hostname and port can get cumbersome, especially
if there are multiple managers. To help with this, a **project name** can be used to identify a TaskVine manager with a human-readable name.
TaskVine workers can then be started for their managers by providing
the project name instead of a host an port number.

The project name feature uses the [Catalog Server](../catalog/index.md) to maintain and track the
project names of managers and their respective locations. It works as follows:
the manager advertises its project name along with its hostname and port to the
catalog server. TaskVine workers that are provided with the manager's project
name query the catalog server to find the hostname and port of the manager with
the given project name.

For example, to have a TaskVine manager advertise its project name as
`myproject`, add the following code snippet after creating the manager:

=== "Python"
    ```python
    m = vine.Manager(name = "myproject")
    ```

=== "C"
    ```C
    vine_set_name(m, "myproject");
    ```

To start a worker for this manager, specify the project name (`myproject`) to
connect in the `-M` option:

```sh
$ vine_worker -M myproject
```


You can start ten workers for this manager on Condor using
`vine_submit_workers` by providing the same option arguments.:

```sh
$ vine_submit_workers -T condor -M myproject 10
Submitting job(s)..........
Logging submit event(s)..........
10 job(s) submitted to cluster 298.
```

Or similarly on UGE using `vine_submit_workers` as:

```sh
$ vine_submit_workers -T uge -M myproject 10
Your job 153097 ("worker.sh") has been submitted
Your job 153098 ("worker.sh") has been submitted
Your job 153099 ("worker.sh") has been submitted
...
```

### TaskVine Online Status Display

An additional benefit of using a project name is that you can
now use the [vine_status](../man_pages/vine_status.md) command
to display the progress of your application.  This shows the name,
location, and statistics of each application that reports itself to the
catalog server.  (Note that this information is updated about once
per minute.).  For example:

```sh
% vine_status
PROJECT               HOST                      PORT WAITING RUNNING COMPLETE WORKERS
molsim-c2h2           home.cse.nd.edu           8999     793      64      791      16
freds-model-search    mars.indiana.edu          9123     100     700     1372     350
yang-analysis-355     login.crc.nd.edu          9100    8932    4873    10007    4873
```

The same information is available in a more graphical form online
at the [TaskVine Online Status](http://ccl.cse.nd.edu/software/taskvine/status),
which looks like this:

<img src=images/vine-status-example.png>

### Managing Workers with the TaskVine Factory

Instead of launching each worker manually from the command line, the utility
**vine_factory** may be used to launch workers as needed. The factory
will submit and maintain a number of workers according to the tasks available
in one or more managers.
For example, we can supply a minimum of 2 workers and a maximum of 10 to
a manager with the project name `myproject` via the condor batch system as follows:

```sh
vine_factory -Tcondor --min-workers=2 --max-workers=10 --manager-name myproject
```

This arguments can be specified in a file. The factory will periodically
re-read this file, which allows adjustments to the number of workers desired:

Configuration file `factory.json`:
```json
{
    "manager-name": "myproject",
    "max-workers": 10,
    "min-workers": 2
}
```
```sh
vine_factory -Tcondor -Cfactory.json
```

For further options, please refer to the TaskVine factory [manual](../man_pages/vine_factory.md).

By default, the factory submits as many tasks that are waiting and running up
to a specified maximum. To run more than one task in a worker, please refer
to the following section on describing [task resources](#task-resources) and [worker resources](#worker-resources).

We can also create a factory directly in python. Creating a factory object does not
immediately launch it, so this is a good time to configure the resources,
number of workers, etc. Factory objects function as Python context managers, so
to indicate that a set of commands should be run with a factory running, wrap
them in a with statement. The factory will be cleaned up automatically at the
end of the block. As an example:

```python
workers = vine.Factory("condor", "myproject")
workers.cores = 4
workers.memory = 4000
workers.disk = 5000
workers.max_workers = 20
with workers:
    while not m.empty():
        t = m.wait(5)
        ...
```

