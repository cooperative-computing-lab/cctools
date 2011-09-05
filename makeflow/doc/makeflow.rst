========
makeflow
========

Workflow engine for executing distributed workflows
---------------------------------------------------

:Author:            Cooperative Computing Lab. http://cse.nd.edu/~ccl.
:Date:              2011-09-05
:Copyright:         GPLv2
:Version:           3.4.0
:Manual section:    1
:Manual group:      cooperative computing tools

SYNOPSIS
========

    makeflow [options] <dagfile>

DESCRIPTION
===========

**Makeflow** is a workflow engine for distributed computing. It accepts a
specification of a large amount of work to be performed, and runs it on remote
machines in parallel where possible. In addition, **Makeflow** is
fault-tolerant, so you can use it to coordinate very large tasks that may run
for days or weeks in the face of failures. **Makeflow** is designed to be
similar to Make, so if you can write a Makefile, then you can write a
**Makeflow**.

You can run a **Makeflow** on your local machine to test it out. If you have a
multi-core machine, then you can run multiple tasks simultaneously. If you have
a Condor pool or a Sun Grid Engine batch system, then you can send your jobs
there to run. If you don't already have a batch system, **Makeflow** comes with
a system called WorkQueue that will let you distribute the load across any
collection of machines, large or small.

OPTIONS
=======

When **makeflow** is ran without arguments, it will attempt to execute the
workflow specified by the **Makeflow** dagfile using the **local** execution
engine.

Commands
~~~~~~~~

    -c              Clean up: remove logfile and all targets.
    -D              Display the Makefile as a Dot graph.
    -k              Syntax check.
    -h              Show this help screen.
    -I              Show input files.
    -O              Show output files.
    -v              Show version string.

Batch Options
~~~~~~~~~~~~~

    -j <#>          Max number of local jobs to run at once. (default is # of cores)
    -J <#>          Max number of remote jobs to run at once. (default is 100)
    -L <logfile>    Use this file for the batch system log. (default is X.<type>log)
    -l <logfile>    Use this file for the makeflow log. (default is X.makeflowlog)
    -B <options>    Add these options to all batch submit files.
    -r <n>          Automatically retry failed batch jobs up to n times.
    -S <timeout>    Time to retry failed batch job submission. (default is 3600s)
    -T <type>       Batch system type: local, condor, sge, moab, wq, hadoop, mpi-queue. (default is local)

Debugging Options
~~~~~~~~~~~~~~~~~
    
    -d <subsystem>  Enable debugging for this subsystem.
    -o <file>       Send debugging to this file.

WorkQueue Options
~~~~~~~~~~~~~~~~~
 
    -a              Advertise the master information to a catalog server.
    -C <catalog>    Set catalog server to <catalog>. Format: HOSTNAME:PORT 
    -e              Set the WorkQueue master to only accept workers that have the same -N <project> option.
    -F <#>          WorkQueue fast abort multiplier. (default is deactivated)
    -N <project>    Set the project name to <project>.
    -P <integer>    Priority. Higher the value, higher the priority.
    -p <port>       Port number to use with WorkQueue . (default is 9123, -1=random)
    -w <mode>       Auto WorkQueue mode. Mode is either 'width' or 'group' (DAG [width] or largest [group] of tasks).
    -W <mode>       WorkQueue scheduling algorithm. (time|files|fcfs)

Other Options
~~~~~~~~~~~~~

    -A              Disable the check for AFS. (experts only)
    -K              Preserve (i.e., do not clean) intermediate symbolic links.
    -z              Force failure on zero-length output files.

ENVIRONMENT VARIABLES
=====================

The following environment variables will affect the execution of your
**Makeflow**:

:BATCH_OPTIONS:

    This corresponds to the **-B <options>** parameter and will pass extra
    batch options to the underlying execution engine.

:MAKEFLOW_MAX_LOCAL_JOBS:
    
    This corresponds to the **-j <#>** parameter and will set the maximum
    number of local batch jobs.  If a **-j <#>** parameter is specified, the
    minimum of the argument and the environment variable is used.

:MAKEFLOW_MAX_REMOTE_JOBS:
    
    This corresponds to the **-J <#>** parameter and will set the maximum
    number of local batch jobs.  If a **-J <#>** parameter is specified, the
    minimum of the argument and the environment variable is used.

Note that variables defined in your **Makeflow** are exported to the
environment.

EXAMPLES
========

::
    
    # Run makeflow locally with debugging
    $ makeflow -d all Makeflow

    # Run makeflow on Condor will special requirements
    $ makeflow -T condor -B "requirements = MachineGroup == 'ccl'" Makeflow
    
    # Run makeflow with WorkQueue using named workers
    $ makeflow -T wq -a -N project.name Makeflow

SEE ALSO
=========

* `Makeflow User's Manual <http://www.cse.nd.edu/~ccl/software/manuals/makeflow.html>`__
