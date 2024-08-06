






















# makeflow(1)

## NAME
**makeflow** - workflow engine for executing distributed workflows

## SYNOPSIS
**makeflow [options] _&lt;dagfile&gt;_**

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

- **-c**,**--clean=_&lt;option&gt;_**<br />Clean up: remove logfile and all targets. If option is one of [intermediates, outputs, cache], only indicated files are removed.
- **-f**,**--summary-log=_&lt;file&gt;_**<br />Write summary of workflow to file.
- **-h**,**--help**<br />Show this help screen.
- **-v**,**--version**<br />Show version string.
- **-X**,**--chdir=_&lt;directory&gt;_**<br />Chdir to enable executing the Makefile in other directory.
- **--argv=_&lt;file&gt;_**<br />Use command line arguments from JSON file.


### Workflow Handling

- **-a**,**--advertise**<br />Advertise the manager information to a catalog server.
- **-l**,**--makeflow-log=_&lt;logfile&gt;_**<br />Use this file for the makeflow log. (default is X.makeflowlog)
- **-L**,**--batch-log=_&lt;logfile&gt;_**<br />Use this file for the batch system log. (default is X._&lt;type&gt;_log)
- **-m**,**--email=_&lt;email&gt;_**<br />Email summary of workflow to address.
- **-j**,**--max-local=_&lt;#&gt;_**<br />Max number of local jobs to run at once. (default is # of cores)
- **-J**,**--max-remote=_&lt;#&gt;_**<br />Max number of remote jobs to run at once. (default is 1000 for -Twq, 100 otherwise)
- **-R**,**--retry**<br />Automatically retry failed batch jobs up to 100 times.
- **-r**,**--retry-count=_&lt;n&gt;_**<br />Automatically retry failed batch jobs up to n times.
- **--local-cores=_&lt;#&gt;_**<br />Max number of cores used for local execution.
- **--local-memory=_&lt;#&gt;_**<br />Max amount of memory used for local execution.
- **--local-disk=_&lt;#&gt;_**<br />Max amount of disk used for local execution.

OPTION_END

### Batch Options

- **-B**,**--batch-options=_&lt;options&gt;_**<br />Add these options to all batch submit files.
- **--send-environment**<br />Send all local environment variables in remote execution.
- **--wait-for-files-upto=_&lt;#&gt;_**<br />Wait for output files to be created upto this many seconds (e.g., to deal with NFS semantics).
- **-S**,**--submission-timeout=_&lt;timeout&gt;_**<br />Time to retry failed batch job submission. (default is 3600s)
- **-T**,**--batch-type=_&lt;type&gt;_**<br />Batch system type: local, dryrun, condor, sge, pbs, torque, blue_waters, slurm, moab, cluster, wq, amazon, mesos. (default is local)
- **--safe-submit-mode**<br />Excludes resources at submission. (SLURM, TORQUE, and PBS)
- **--ignore-memory-spec**<br />Excludes memory at submission. (SLURM)
- **--batch-mem-type=_&lt;type&gt;_**<br />Specify memory resource type. (SGE)
- **--working-dir=_&lt;dir|url&gt;_**<br />Working directory for batch system.
- **--sandbox**<br />Run task in sandbox using bash script and task directory.
- **--verbose-jobnames**<br />Set the job name based on the command.
- **--keep-wrapper-stdout**<br />Do not redirect to /dev/null the stdout file from the batch system.


### JSON/JX Options

- **--json**<br />Interpret _&lt;dagfile&gt;_ as a JSON format Makeflow.
- **--jx**<br />Evaluate JX expressions in _&lt;dagfile&gt;_. Implies --json.
- **--jx-args=_&lt;args&gt;_**<br />Read variable definitions from the JX file _&lt;args&gt;_.
- **--jx-define=_&lt;VAL=EXPR&gt;_**<br />Set the variable _&lt;VAL&gt;_ to the JX expression _&lt;EXPR&gt;_.
- **--jx-context=_&lt;ctx&gt;_**<br />Deprecated. See '--jx-args'.


### Debugging Options

- **-d**,**--debug=_&lt;subsystem&gt;_**<br />Enable debugging for this subsystem.
- **-o**,**--debug-file=_&lt;file&gt;_**<br />Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **--debug-rotate-max=_&lt;byte&gt;_**<br />Rotate debug file once it reaches this size.
- **--verbose**<br />Display runtime progress on stdout.


### TaskVine and Work Queue Options

- **-C**,**--catalog-server=_&lt;catalog&gt;_**<br />Set catalog server to _&lt;catalog&gt;_. Format: HOSTNAME:PORT
- **-F**,**--wq-fast-abort=_&lt;#&gt;_**<br />WorkQueue fast abort multiplier. (default is deactivated)
- **-M**,**--project-name=_&lt;project&gt;_**<br />Set the project name to _&lt;project&gt;_.
- **-p**,**--port=_&lt;port&gt;_**<br />Port number to use with WorkQueue. (default is 9123, 0=arbitrary)
- **-Z**,**--port-file=_&lt;file&gt;_**<br />Select port at random and write it to this file.  (default is disabled)
- **-P**,**--priority=_&lt;integer&gt;_**<br />Priority. Higher the value, higher the priority.
- **-t**,**--keepalive-timeout=_&lt;#&gt;_**<br />Work Queue keepalive timeout (default: 30s)
- **-u**,**--keepalive-interval=_&lt;#&gt;_**<br />Work Queue keepalive interval (default: 120s)
- **-W**,**--schedule=_&lt;mode&gt;_**<br />WorkQueue scheduling algorithm. (time|files|fcfs)
- **--password=_&lt;pwfile&gt;_**<br />Password file for authenticating workers.
- **--ssl-cert=_&lt;&gt;_**<br /> Set the SSL certificate file for encrypting connection.
- **--ssl-key=_&lt;&gt;_**<br /> Set the SSL certificate file for encrypting connection.
- **--cache-mode**<br /> Control worker caching mode. (never|workflow|forever)
- **--preferred-connection=_&lt;connection&gt;_**<br />Indicate preferred connection. Chose one of by_ip or by_hostname. (default is by_ip)


### Monitor Options

- **--monitor=_&lt;dir&gt;_**<br />Enable the resource monitor, and write the monitor logs to _&lt;dir&gt;_
- **--monitor=_&lt;dir&gt;_**<br />Enable the resource monitor, and write the monitor logs to _&lt;dir&gt;_
- **--monitor-exe=_&lt;file&gt;_**<br />Specify resource monitor executable.
- **--monitor-with-time-series**<br />Enable monitor time series.                 (default is disabled)
- **--monitor-with-opened-files**<br />Enable monitoring of openened files.        (default is disabled)
- **--monitor-interval=_&lt;n&gt;_**<br />Set monitor interval to _&lt;n&gt;_ seconds. (default 1 second)
- **--monitor-log-fmt=_&lt;fmt&gt;_**<br />Format for monitor logs. (default resource-rule-%06.6d, %d -> rule number)
- **--monitor-measure-dir**<br />Monitor measures the task's current execution directory size.
- **--allocation=_&lt;waste&gt;_**<br />When monitoring is enabled, automatically assign resource allocations to tasks. Makeflow will try to minimize **waste** or maximize **throughput**.


### Umbrella Options

- **--umbrella-binary=_&lt;filepath&gt;_**<br />Umbrella binary for running every rule in a makeflow.
- **--umbrella-log-prefix=_&lt;filepath&gt;_**<br />Umbrella log file prefix for running every rule in a makeflow. (default is _&lt;makefilename&gt;_.umbrella.log)
- **--umbrella-log-prefix=_&lt;filepath&gt;_**<br />Umbrella log file prefix for running every rule in a makeflow. (default is _&lt;makefilename&gt;_.umbrella.log)
- **--umbrella-mode=_&lt;mode&gt;_**<br />Umbrella execution mode for running every rule in a makeflow. (default is local)
- **--umbrella-spec=_&lt;filepath&gt;_**<br />Umbrella spec for running every rule in a makeflow.


### Docker Support

- **--docker=_&lt;image&gt;_**<br /> Run each task in the Docker container with this name.  The image will be obtained via "docker pull" if it is not already available.
- **--docker-tar=_&lt;tar&gt;_**<br /> Run each task in the Docker container given by this tar file.  The image will be uploaded via "docker load" on each execution site.
- **--docker-opt=_&lt;string&gt;_**<br />Specify options to be used in DSingularityocker execution.


### Singularity Support

- **--singularity=_&lt;image&gt;_**<br /> Run each task in the Singularity container with this name.  The container will be created from the passed in image.
- **--singularity-opt=_&lt;string&gt;_**<br />Specify options to be used in Singularity execution.


### Amazon Options

- **--amazon-config=_&lt;path&gt;_**<br /> Path to Amazon EC2 configuration file generated by makeflow_ec2_setup.


### Kubernetes Options

- **--k8s-image=_&lt;docker_image&gt;_**<br /> Indicate the Docker image for running pods on Kubernetes cluster. 


### Mountfile Support

- **--mounts=_&lt;mountfile&gt;_**<br />Use this file as a mountlist. Every line of a mountfile can be used to specify the source and target of each input dependency in the format of **target source** (Note there should be a space between target and source.).
- **--cache=_&lt;cache_dir&gt;_**<br />Use this dir as the cache for file dependencies.


### Archiving Options

- **--archive=_&lt;&gt;_**<br />Archive results of workflow at the specified path (by default /tmp/makeflow.archive.$UID) and use outputs of any archived jobs instead of re-executing job
- **--archive-dir=_&lt;path&gt;_**<br />Specify archive base directory.
- **--archive-read=_&lt;path&gt;_**<br />Only check to see if jobs have been cached and use outputs if it has been
- **--archive-s3=_&lt;s3_bucket&gt;_**<br />Base S3 Bucket name
- **--archive-s3-no-check=_&lt;s3_bucket&gt;_**<br />Blind upload files to S3 bucket (No existence check in bucket).
- **--s3-hostname=_&lt;s3_hostname&gt;_**<br />Base S3 hostname. Used for AWS S3.
- **--s3-keyid=_&lt;s3_key_id&gt;_**<br />Access Key for cloud server. Used for AWS S3.
- **--s3-secretkey=_&lt;secret_key&gt;_**<br />Secret Key for cloud server. Used for AWS S3.


### VC3 Builder Options

- **--vc3-builder**<br />Enable VC3 Builder
- **--vc3-exe=_&lt;file&gt;_**<br />VC3 Builder executable location
- **--vc3-log=_&lt;file&gt;_**<br />VC3 Builder log name
- **--vc3-options=_&lt;string&gt;_**<br />VC3 Builder option string


### Other Options

- **-A**,**--disable-afs-check**<br />Disable the check for AFS. (experts only)
- **-z**,**--zero-length-error**<br />Force failure on zero-length output files.
- **-g**,**--gc=_&lt;type&gt;_**<br />Enable garbage collection. (ref_cnt|on_demand|all)
- **--gc-size=_&lt;int&gt;_**<br />Set disk size to trigger GC. (on_demand only)
- **-G**,**--gc-count=_&lt;int&gt;_**<br />Set number of files to trigger GC. (ref_cnt only)
- **--wrapper=_&lt;script&gt;_**<br /> Wrap all commands with this **script**. Each rule's original recipe is appended to **script** or replaces the first occurrence of **{}** in **script**.
- **--wrapper-input=_&lt;file&gt;_**<br /> Wrapper command requires this input file. This option may be specified more than once, defining an array of inputs. Additionally, each job executing a recipe has a unique integer identifier that replaces occurrences **%%** in **file**.
- **--wrapper-output=_&lt;file&gt;_**<br /> Wrapper command requires this output file. This option may be specified more than once, defining an array of outputs. Additionally, each job executing a recipe has a unique integer identifier that replaces occurrences **%%** in **file**.
- **--enforcement**<br />Use Parrot to restrict access to the given inputs/outputs.
- **--parrot-path=_&lt;path&gt;_**<br />Path to parrot_run executable on the host system.
- **--env-replace-path=_&lt;path&gt;_**<br />Path to env_replace executable on the host system.
- **--skip-file-check**<br />Do not check for file existence before running.
- **--do-not-save-failed-output**<br />Disable saving failed nodes to directory for later analysis.
- **--shared-fs=_&lt;dir&gt;_**<br />Assume the given directory is a shared filesystem accessible at all execution sites.
- **-X**,**--change-directory=_&lt;dir&gt;_**<br />Change to _&lt;dir&gt;_ prior to executing the workflow.
- **-X**,**--change-directory=_&lt;dir&gt;_**<br />Change to _&lt;dir&gt;_ prior to executing the workflow.


## DRYRUN MODE

When the batch system is set to **-T** _&lt;dryrun&gt;_, Makeflow runs as usual
but does not actually execute jobs or modify the system. This is useful to
check that wrappers and substitutions are applied as expected. In addition,
Makeflow will write an equivalent shell script to the batch system log
specified by **-L** _&lt;logfile&gt;_. This script will run the commands in
serial that Makeflow would have run. This shell script format may be useful
for archival purposes, since it does not depend on Makeflow.

## ENVIRONMENT VARIABLES

The following environment variables will affect the execution of your
**Makeflow**:
### BATCH_OPTIONS

This corresponds to the **-B** _&lt;options&gt;_ parameter and will pass extra
batch options to the underlying execution engine.

### MAKEFLOW_MAX_LOCAL_JOBS
This corresponds to the **-j** _&lt;#&gt;_ parameter and will set the maximum
number of local batch jobs.  If a **-j** _&lt;#&gt;_ parameter is specified, the
minimum of the argument and the environment variable is used.

### MAKEFLOW_MAX_REMOTE_JOBS
This corresponds to the **-J** _&lt;#&gt;_ parameter and will set the maximum
number of local batch jobs.  If a **-J** _&lt;#&gt;_ parameter is specified, the
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
makeflow -T wq -a -M project.name Makeflow
```

Create a directory containing all of the dependencies required to run the
specified makeflow
```
makeflow -b bundle Makeflow
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Makeflow User Manual]("../makeflow.html")
- [makeflow(1)](makeflow.md) [makeflow_monitor(1)](makeflow_monitor.md) [makeflow_analyze(1)](makeflow_analyze.md) [makeflow_viz(1)](makeflow_viz.md) [makeflow_graph_log(1)](makeflow_graph_log.md) [starch(1)](starch.md) [makeflow_ec2_setup(1)](makeflow_ec2_setup.md) [makeflow_ec2_cleanup(1)](makeflow_ec2_cleanup.md)


CCTools
