






















# vine_status(1)

## NAME
**vine_status** - display status of currently running TaskVine applications.

## SYNOPSIS
**vine_status [options] [manager] [port]**

## DESCRIPTION

**vine_status** displays the status of currently running TaskVine applications.
When run with no options, it queries the global catalog server to list the currently running
TaskVine managers.  When given an address and a port, it queries a manager directly to obtain
more detailed information about tasks and workers.


- Hostname and port number of the application.
- Number of waiting tasks.
- Number of completed tasks.
- Number of connected workers.
- Number of tasks currently being executed.
- Timestamp of when there was last communication with the application.


## OPTIONS

- **--where=_&lt;expr&gt;_**<br /> Show only TaskVine managers matching this expression.
- **-Q**,**--statistics**<br />Show summary information about managers. (default)
- **-M**,**--project-name=_&lt;name&gt;_**<br />Filter results of -Q for managers matching _&lt;name&gt;_.
- **-W**,**--workers**<br />Show details of all workers connected to the manager.
- **-T**,**--tasks**<br />Show details of all tasks known by the manager.
- **-A**,**--able-workers**<br />List categories of the given manager, size of largest task, and workers that can run it.
- **-R**,**--resources**<br />Show available resources for each manager.
- **--capacity**<br />Show resource capacities for each manager.
- **-l**,**--verbose**<br />Long output.
- **-C**,**--catalog=_&lt;catalog&gt;_**<br />Set catalog server to _&lt;catalog&gt;_. Format: HOSTNAME:PORT
- **-C**,**--catalog=_&lt;catalog&gt;_**<br />Set catalog server to _&lt;catalog&gt;_. Format: HOSTNAME:PORT
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for the given subsystem. Try -d all as a start.
- **-t**,**--timeout=_&lt;time&gt;_**<br />RPC timeout (default=300s).
- **-o**,**--debug-file=_&lt;file&gt;_**<br />Send debugging to this file. (can also be :stderr, or :stdout)
- **-O**,**--debug-rotate-max=_&lt;bytes&gt;_**<br />Rotate debug file once it reaches this size.
- **-v**,**--version**<br />Show vine_status version.
- **-h**,**--help**<br />Show this help message.


## EXAMPLES

Without arguments, **vine_status** shows a summary of all of the
projects currently reporting to the default catalog server. Waiting, running,
and complete columns refer to number of tasks:

```
$ vine_status
PROJECT            HOST                   PORT WAITING RUNNING COMPLETE WORKERS
shrimp             cclws16.cse.nd.edu     9001     963      37        2      33
crustacea          terra.solar.edu        9000       0    2310    32084     700
```

With the **-R** option, a summary of the resources available to each manager is shown:

```
$ vine_status -R
MANAGER                         CORES      MEMORY          DISK
shrimp                         228        279300          932512
crustacea                      4200       4136784         9049985
```

With the **--capacity** option, a summary of the resource capacities for each manager is shown:

```
$ vine_status --capacity
MANAGER                         TASKS      CORES      MEMORY          DISK
refine                         ???        ???        ???             ???
shrimp                         99120      99120      781362960       1307691584
crustacea                      318911     318911     326564864       326564864
```

Use the **-W** option to list the workers connected to a particular manager.
Completed and running columns refer to numbers of tasks:

```
$ vine_status -W cclws16.cse.nd.edu 9001
HOST                     ADDRESS          COMPLETED RUNNING
mccourt02.helios.nd.edu  10.176.48.102:40         0 4
cse-ws-23.cse.nd.edu     129.74.155.120:5         0 0
mccourt50.helios.nd.edu  10.176.63.50:560         4 4
dirt02.cse.nd.edu        129.74.20.156:58         4 4
cclvm03.virtual.crc.nd.e 129.74.246.235:3         0 0
```

With the **-T** option, a list of all the tasks submitted to the manager is shown:

```
$ vine_status -T cclws16.cse.nd.edu 9001
ID       STATE    PRIORITY HOST                     COMMAND
1000     WAITING         0 ???                      ./rmapper-cs -M fast -M 50bp 1
999      WAITING         0 ???                      ./rmapper-cs -M fast -M 50bp 1
21       running         0 cse-ws-35.cse.nd.edu     ./rmapper-cs -M fast -M 50bp 3
20       running         0 cse-ws-35.cse.nd.edu     ./rmapper-cs -M fast -M 50bp 2
19       running         0 cse-ws-35.cse.nd.edu     ./rmapper-cs -M fast -M 50bp 2
18       running         0 cse-ws-35.cse.nd.edu     ./rmapper-cs -M fast -M 50bp 2
...
```


The **-A** option shows a summary of the resources observed per task
category.

```
$ vine_status -A cclws16.cse.nd.edu 9001
CATEGORY        RUNNING    WAITING  FIT-WORKERS  MAX-CORES MAX-MEMORY   MAX-DISK
analysis            216        784           54          4      ~1011      ~3502
merge                20         92           30         ~1      ~4021      21318
default               1         25           54         >1       ~503       >243
```

With the -A option:

- Running and waiting correspond to number of tasks in the category.
- Fit-workers shows the number of connected workers able to run the largest task in the category.
- max-cores, max-memory, and max-disk show the corresponding largest value in the category.


The value may have the following prefixes:

- No prefix. The maximum value was manually specified.
- ~ All the task have run with at most this quantity of resources.
- ＞ There is at least one task that has used more than this quantity of resources, but the maximum remains unknown.



Finally, the **-l** option shows statistics of the manager in a JSON object:

```
$ vine_status -l cclws16.cse.nd.edu 9001
{"categories":[{"max_disk":"3500","max_memory":"1024","max_cores":"1",...
```

## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [TaskVine User Manual]("../taskvine.html")
- [vine_worker(1)](vine_worker.md) [vine_status(1)](vine_status.md) [vine_factory(1)](vine_factory.md) [vine_graph_log(1)](vine_graph_log.md) 


CCTools
