






















# makeflow(1)

## NAME
**makeflow** - workflow engine for executing distributed workflows

## SYNOPSIS
****makeflow [options] <dagfile>****

## DESCRIPTION

**Makeflow** is a workflow engine for distributed computing. It accepts a
specification of a large amount of work to be performed, and runs it on remote
machines in parallel where possible. In addition, **Makeflow** is
fault-tolerant, so you can use it to coordinate very large tasks that may run
for days or weeks in the face of failures. **Makeflow** is designed to be
similar to Make, so if you can write a Makefile, then you can write a
**Makeflow**.



You can run a **Makeflow** on your local machine to test it out. If you have
a multi-core machine, then you can run multiple tasks simultaneously. If you
have a Condor pool or a Sun Grid Engine batch system, then you can send your
jobs there to run. If you don't already have a batch system, **Makeflow**
comes with a system called Work Queue that will let you distribute the load
across any collection of machines, large or small.  **Makeflow** also
supports execution in a Docker container, regardless of the batch system
used.



## OPTIONS
When **makeflow** is ran without arguments, it will attempt to execute the
workflow specified by the **Makeflow** dagfile using the **local**
execution engine.

### Commands

- **-c ----clean <option>** Clean up: remove logfile and all targets. If option is one of [intermediates, outputs, cache], only indicated files are removed.
- **-f --summary-log <file>** Write summary of workflow to file.
- **-h, --help** Show this help screen.
- **-v, --version** Show version string.
- **-X --chdir <directory>** Chdir to enable executing the Makefile in other directory.
- **--argv file** Use command line arguments from JSON file.


### Workflow Handling

- **-a, --advertise** Advertise the master information to a catalog server.
- **-l --makeflow-log <logfile>** Use this file for the makeflow log. (default is X.makeflowlog)
- **-L --batch-log <logfile>** Use this file for the batch system log. (default is X.<type>log)
- **-m --email <email>** Email summary of workflow to address.
- **-j --max-local <#>** Max number of local jobs to run at once. (default is # of cores)
- **-J --max-remote <#>** Max number of remote jobs to run at once. (default is 1000 for -Twq, 100 otherwise)
- **-R, --retry** Automatically retry failed batch jobs up to 100 times.
- **-r --retry-count <n>** Automatically retry failed batch jobs up to n times.
- **--local-cores #** Max number of cores used for local execution.
- **--local-memory #** Max amount of memory used for local execution.
- **--local-disk #** Max amount of disk used for local execution.

OPTION_END

### Batch Options

- **-B --batch-options <options>** Add these options to all batch submit files.
- **--send-environment** Send all local environment variables in remote execution.
- **--wait-for-files-upto #** Wait for output files to be created upto this many seconds (e.g., to deal with NFS semantics).
- **-S --submission-timeout <timeout>** Time to retry failed batch job submission. (default is 3600s)
- **-T --batch-type <type>** Batch system type: local, dryrun, condor, sge, pbs, torque, blue_waters, slurm, moab, cluster, wq, amazon, mesos. (default is local)
- **--safe-submit-mode** Excludes resources at submission. (SLURM, TORQUE, and PBS)
- **--ignore-memory-spec** Excludes memory at submission. (SLURM)
- **--batch-mem-type type** Specify memory resource type. (SGE)
- **--working-dir dir|url** Working directory for batch system.
- **--sandbox** Run task in sandbox using bash script and task directory.
- **--verbose-jobnames** Set the job name based on the command.


### JSON/JX Options

- **--json** Interpret <dagfile> as a JSON format Makeflow.
- **--jx** Evaluate JX expressions in <dagfile>. Implies --json.
- **--jx-args args** Read variable definitions from the JX file <args>.
- **--jx-define VAL=EXPR** Set the variable <VAL> to the JX expression <EXPR>.
- **--jx-context ctx** Deprecated. See '--jx-args'.


### Debugging Options

- **-d --debug <subsystem>** Enable debugging for this subsystem.
- **-o --debug-file <file>** Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **--debug-rotate-max byte** Rotate debug file once it reaches this size.
- **--verbose** Display runtime progress on stdout.


### WorkQueue Options

- **-C --catalog-server <catalog>** Set catalog server to <catalog>. Format: HOSTNAME:PORT
- **-F --wq-fast-abort <#>** WorkQueue fast abort multiplier. (default is deactivated)
- **-M ---N <project-name>** Set the project name to <project>.
- **-p --port <port>** Port number to use with WorkQueue. (default is 9123, 0=arbitrary)
- **-Z --port-file <file>** Select port at random and write it to this file.  (default is disabled)
- **-P --priority <integer>** Priority. Higher the value, higher the priority.
- **-t --wq-keepalive-timeout <#>** Work Queue keepalive timeout (default: 30s)
- **-u --wq-keepalive-interval <#>** Work Queue keepalive interval (default: 120s)
- **-W --wq-schedule <mode>** WorkQueue scheduling algorithm. (time|files|fcfs)
- **password pwfile** Password file for authenticating workers.
- **--disable-cache** Disable file caching (currently only Work Queue, default is false)
- **--work-queue-preferred-connection connection** Indicate preferred connection. Chose one of by_ip or by_hostname. (default is by_ip)


### Monitor Options

- **--monitor dir** Enable the resource monitor, and write the monitor logs to <dir>
- **--monitor-exe file** Specify resource monitor executable.
- **--monitor-with-time-series** Enable monitor time series.                 (default is disabled)
- **--monitor-with-opened-files** Enable monitoring of openened files.        (default is disabled)
- **--monitor-interval #** Set monitor interval to <#> seconds. (default 1 second)
- **--monitor-log-fmt fmt** Format for monitor logs. (default resource-rule-%06.6d, %d -> rule number)
- **--monitor-measure-dir** Monitor measures the task's current execution directory size.
- **--allocation waste** When monitoring is enabled, automatically assign resource allocations to tasks. Makeflow will try to minimize **waste** or maximize **throughput**.


### Umbrella Options

- **--umbrella-binary filepath** Umbrella binary for running every rule in a makeflow.
- **--umbrella-log-prefix filepath** Umbrella log file prefix for running every rule in a makeflow. (default is <makefilename>.umbrella.log)
- **--umbrella-mode mode** Umbrella execution mode for running every rule in a makeflow. (default is local)
- **--umbrella-spec filepath** Umbrella spec for running every rule in a makeflow.


### Docker Support

- **--docker image**  Run each task in the Docker container with this name.  The image will be obtained via "docker pull" if it is not already available.
- **--docker-tar tar**  Run each task in the Docker container given by this tar file.  The image will be uploaded via "docker load" on each execution site.
- **--docker-opt string** Specify options to be used in DSingularityocker execution.


### Singularity Support

- **--singularity image**  Run each task in the Singularity container with this name.  The container will be created from the passed in image.
- **--singularity-opt string** Specify options to be used in Singularity execution.


### Amazon Options

- **--amazon-config path**  Path to Amazon EC2 configuration file generated by makeflow_ec2_setup.


### Amazon Lambda Options

- **--lambda-config path**  Path to Amazon Lambda configuration file generated by makeflow_lambda_setup.


### Amazon Batch Options

- **--amazon-batch-config path**  Path to Amazon Batch configuration file generated by makeflow_amazon_batch_setup.
- **--amazon-batch-img img**  Specify image used for Amazon Batch execution.


### Mesos Options

- **--mesos-master hostname**  Indicate the host name of preferred mesos master.
- **--mesos-path filepath**  Indicate the path to mesos python2 site-packages.
- **--mesos-preload library**  Indicate the linking libraries for running mesos..

### Kubernetes Options

- **--k8s-image docker_image**  Indicate the Docker image for running pods on Kubernetes cluster. 


### Mountfile Support

- **--mounts mountfile** Use this file as a mountlist. Every line of a mountfile can be used to specify the source and target of each input dependency in the format of **target source** (Note there should be a space between target and source.).
- **--cache cache_dir** Use this dir as the cache for file dependencies.


### Archiving Options

- **--archive ** Archive results of workflow at the specified path (by default /tmp/makeflow.archive.$UID) and use outputs of any archived jobs instead of re-executing job
- **--archive-dir path** Specify archive base directory.
- **--archive-read path** Only check to see if jobs have been cached and use outputs if it has been
- **--archive-s3 s3_bucket** Base S3 Bucket name
- **--archive-s3-no-check s3_bucket** Blind upload files to S3 bucket (No existence check in bucket).
- **--s3-hostname s3_hostname** Base S3 hostname. Used for AWS S3.
- **--s3-keyid s3_key_id** Access Key for cloud server. Used for AWS S3.
- **--s3-secretkey secret_key** Secret Key for cloud server. Used for AWS S3.


### VC3 Builder Options

- **--vc3-builder** Enable VC3 Builder
- **--vc3-exe file** VC3 Builder executable location
- **--vc3-log file** VC3 Builder log name
- **--vc3-options string** VC3 Builder option string


### Other Options

- **-A, --disable-afs-check** Disable the check for AFS. (experts only)
- **-z, --zero-length-error** Force failure on zero-length output files.
- **-g --gc <type>** Enable garbage collection. (ref_cnt|on_demand|all)
- **--gc-size int** Set disk size to trigger GC. (on_demand only)
- **-G --gc-count <int>** Set number of files to trigger GC. (ref_cnt only)
- **--wrapper script**  Wrap all commands with this **script**. Each rule's original recipe is appended to **script** or replaces the first occurrence of **{}** in **script**.
- **--wrapper-input file**  Wrapper command requires this input file. This option may be specified more than once, defining an array of inputs. Additionally, each job executing a recipe has a unique integer identifier that replaces occurrences **%%** in **file**.
- **--wrapper-output file**  Wrapper command requires this output file. This option may be specified more than once, defining an array of outputs. Additionally, each job executing a recipe has a unique integer identifier that replaces occurrences **%%** in **file**.
- **--enforcement** Use Parrot to restrict access to the given inputs/outputs.
- **--parrot-path path** Path to parrot_run executable on the host system.
- **--env-replace-path path** Path to env_replace executable on the host system.
- **--skip-file-check** Do not check for file existence before running.
- **--do-not-save-failed-output** Disable saving failed nodes to directory for later analysis.
- **--shared-fs dir** Assume the given directory is a shared filesystem accessible at all execution sites.
- **-X --change-directory <dir>** Change to <dir> prior to executing the workflow.


### MPI Options

- **--mpi-cores #** Number of cores each MPI worker uses.
- **--mpi-memory #** Amount of memory each MPI worker uses.
- **--mpi-task-working-dir path** Path to the MPI tasks working directory.


## DRYRUN MODE

When the batch system is set to **-T** <dryrun>, Makeflow runs as usual
but does not actually execute jobs or modify the system. This is useful to
check that wrappers and substitutions are applied as expected. In addition,
Makeflow will write an equivalent shell script to the batch system log
specified by **-L** <logfile>. This script will run the commands in
serial that Makeflow would have run. This shell script format may be useful
for archival purposes, since it does not depend on Makeflow.

## MPI
When cctools is built with --with-mpicc-path=which mpicc` configuration, Makeflow can be ran as an MPI program.
To do so, run Makeflow as an argument to BOLD(mpirun)/BOLD(mpiexec) and set BOLD(-T) PARAM(mpi) as a Makeflow option.
When submitting mpi, request one process per core. Makeflow will count up how many processes each node given to MPI
has, and use that as the core count for the worker on that node. Makeflow will then share memory evenly amongst the cores
on the node, following the following equation BOLD(worker_memory) = (BOLD(total_memory) / BOLD(total_logical_cores)) * BOLD(num_cores_for_worker).
To override Makeflow sharing memory equally, or setting per-worker cores value, use OPTION_ITEM('--mpi-cores) and - **'--mpi-memory'** .

Tasks can also have their own sandbox. To specify the directory for tasks to create their sandbox subdirectory in, use - **'--mpi-task-working-dir'** .

## ENVIRONMENT VARIABLES

The following environment variables will affect the execution of your
**Makeflow**:
### BATCH_OPTIONS

This corresponds to the **-B** <options> parameter and will pass extra
batch options to the underlying execution engine.

### MAKEFLOW_MAX_LOCAL_JOBS
This corresponds to the **-j** <#> parameter and will set the maximum
number of local batch jobs.  If a **-j** <#> parameter is specified, the
minimum of the argument and the environment variable is used.

### MAKEFLOW_MAX_REMOTE_JOBS
This corresponds to the **-J** <#> parameter and will set the maximum
number of local batch jobs.  If a **-J** <#> parameter is specified, the
minimum of the argument and the environment variable is used.

Note that variables defined in your **Makeflow** are exported to the
environment.

### TCP_LOW_PORT
Inclusive low port in range used with **-p 0**.

### TCP_HIGH_PORT)
Inclusive high port in range used with **-p 0**.

## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

Run makeflow locally with debugging:
```
makeflow -d all Makeflow
```

Run makeflow on Condor will special requirements:
```
makeflow -T condor -B "requirements = MachineGroup == 'ccl'" Makeflow
```

Run makeflow with WorkQueue using named workers:
```
makeflow -T wq -a -N project.name Makeflow
```

Create a directory containing all of the dependencies required to run the
specified makeflow
```
makeflow -b bundle Makeflow
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2019 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Makeflow User Manual]("../makeflow.html")
- [makeflow(1)](makeflow.md) [makeflow_monitor(1)](makeflow_monitor.md) [makeflow_analyze(1)](makeflow_analyze.md) [makeflow_viz(1)](makeflow_viz.md) [makeflow_graph_log(1)](makeflow_graph_log.md) [starch(1)](starch.md) [makeflow_ec2_setup(1)](makeflow_ec2_setup.md) [makeflow_ec2_cleanup(1)](makeflow_ec2_cleanup.md) [makeflow_ec2_estimate(1)](makeflow_ec2_estimate.md)


CCTools 8.0.0 DEVELOPMENT released on
