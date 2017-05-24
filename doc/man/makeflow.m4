include(manual.h)dnl
HEADER(makeflow)

SECTION(NAME)
BOLD(makeflow) - workflow engine for executing distributed workflows

SECTION(SYNOPSIS)
CODE(BOLD(makeflow [options] PARAM(dagfile)))

SECTION(DESCRIPTION)

BOLD(Makeflow) is a workflow engine for distributed computing. It accepts a
specification of a large amount of work to be performed, and runs it on remote
machines in parallel where possible. In addition, BOLD(Makeflow) is
fault-tolerant, so you can use it to coordinate very large tasks that may run
for days or weeks in the face of failures. BOLD(Makeflow) is designed to be
similar to Make, so if you can write a Makefile, then you can write a
BOLD(Makeflow).

PARA

You can run a BOLD(Makeflow) on your local machine to test it out. If you have
a multi-core machine, then you can run multiple tasks simultaneously. If you
have a Condor pool or a Sun Grid Engine batch system, then you can send your
jobs there to run. If you don't already have a batch system, BOLD(Makeflow)
comes with a system called Work Queue that will let you distribute the load
across any collection of machines, large or small.  BOLD(Makeflow) also
supports execution in a Docker container, regardless of the batch system
used.

PARA

SECTION(OPTIONS)
When CODE(makeflow) is ran without arguments, it will attempt to execute the
workflow specified by the BOLD(Makeflow) dagfile using the CODE(local)
execution engine.

SUBSECTION(Commands)
OPTIONS_BEGIN
OPTION_TRIPLET(-c, --clean, option)Clean up: remove logfile and all targets. If option is one of [intermediates, outputs, cache], only indicated files are removed.
OPTION_TRIPLET(-f, summary-log, file)Write summary of workflow to file.
OPTION_ITEM(`-h, --help')Show this help screen.
OPTION_TRIPLET(-m, email, email)Email summary of workflow to address.
OPTION_ITEM(`-v, --version')Show version string.
OPTION_TRIPLET(-X, chdir, directory)Chdir to enable executing the Makefile in other directory.
OPTIONS_END

SUBSECTION(Batch Options)
OPTIONS_BEGIN
OPTION_TRIPLET(-B, batch-options, options)Add these options to all batch submit files.
OPTION_TRIPLET(-j, max-local, #)Max number of local jobs to run at once. (default is # of cores)
OPTION_TRIPLET(-J, max-remote, #)Max number of remote jobs to run at once. (default is 1000 for -Twq, 100 otherwise)
OPTION_TRIPLET(-l, makeflow-log, logfile)Use this file for the makeflow log. (default is X.makeflowlog)
OPTION_TRIPLET(-L, batch-log, logfile)Use this file for the batch system log. (default is X.PARAM(type)log)
OPTION_ITEM(`-R, --retry')Automatically retry failed batch jobs up to 100 times.
OPTION_TRIPLET(-r, retry-count, n)Automatically retry failed batch jobs up to n times.
OPTION_PAIR(--wait-for-files-upto, #)Wait for output files to be created upto this many seconds (e.g., to deal with NFS semantics).
OPTION_TRIPLET(-S, submission-timeout, timeout)Time to retry failed batch job submission. (default is 3600s)
OPTION_TRIPLET(-T, batch-type, type)Batch system type: local, dryrun, condor, sge, pbs, torque, blue_waters, slurm, moab, cluster, wq, amazon, mesos. (default is local)
OPTIONS_END

SUBSECTION(JSON/JX Options)
OPTIONS_BEGIN
OPTION_ITEM(--json)Interpret PARAM(dagfile) as a JSON format Makeflow.
OPTION_ITEM(--jx)Evaluate JX expressions in PARAM(dagfile). Implies --json.
OPTION_PAIR(--jx-context, ctx)Use PARAM(ctx) as the context for evaluating JX.
OPTIONS_END

SUBSECTION(Debugging Options)
OPTIONS_BEGIN
OPTION_TRIPLET(-d, debug, subsystem)Enable debugging for this subsystem.
OPTION_TRIPLET(-o,debug-file,file)Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs be sent to stdout (":stdout"), to the system syslog (":syslog"), or to the systemd journal (":journal").
OPTION_ITEM(`--verbose')Display runtime progress on stdout.
OPTIONS_END

SUBSECTION(WorkQueue Options)
OPTIONS_BEGIN
OPTION_ITEM(`-a, --advertise')Advertise the master information to a catalog server.
OPTION_TRIPLET(-C, catalog-server, catalog)Set catalog server to PARAM(catalog). Format: HOSTNAME:PORT
OPTION_TRIPLET(-F, wq-fast-abort, #)WorkQueue fast abort multiplier. (default is deactivated)
OPTION_TRIPLET(-M,-N, project-name, project)Set the project name to PARAM(project).
OPTION_TRIPLET(-p, port, port)Port number to use with WorkQueue. (default is 9123, 0=arbitrary)
OPTION_TRIPLET(-Z, port-file, file)Select port at random and write it to this file.  (default is disabled)
OPTION_TRIPLET(-P, priority, integer)Priority. Higher the value, higher the priority.
OPTION_TRIPLET(-W, wq-schedule, mode)WorkQueue scheduling algorithm. (time|files|fcfs)
OPTION_TRIPLET(-s, password, pwfile)Password file for authenticating workers.
OPTION_ITEM(`--disable-cache')Disable file caching (currently only Work Queue, default is false)
OPTION_PAIR(--work-queue-preferred-connection,connection)Indicate preferred connection. Chose one of by_ip or by_hostname. (default is by_ip)
OPTIONS_END

SUBSECTION(Monitor Options)
OPTIONS_BEGIN
OPTION_PAIR(--monitor, dir)Enable the resource monitor, and write the monitor logs to <dir>
OPTION_ITEM(`--monitor-with-time-series')Enable monitor time series.                 (default is disabled)
OPTION_ITEM(`--monitor-with-opened-files')Enable monitoring of openened files.        (default is disabled)
OPTION_PAIR(--monitor-interval, #)Set monitor interval to <#> seconds. (default 1 second)
OPTION_PAIR(--monitor-log-fmt, fmt)Format for monitor logs. (default resource-rule-%06.6d, %d -> rule number)
OPTION_PAIR(--allocation, waste,throughput)When monitoring is enabled, automatically assign resource allocations to tasks. Makeflow will try to minimize CODE(waste) or maximize CODE(throughput).
OPTIONS_END

SUBSECTION(Umbrella Options)
OPTIONS_BEGIN
OPTION_PAIR(--umbrella-binary, filepath)Umbrella binary for running every rule in a makeflow.
OPTION_PAIR(--umbrella-log-prefix, filepath)Umbrella log file prefix for running every rule in a makeflow. (default is <makefilename>.umbrella.log)
OPTION_PAIR(--umbrella-mode, mode)Umbrella execution mode for running every rule in a makeflow. (default is local)
OPTION_PAIR(--umbrella-spec, filepath)Umbrella spec for running every rule in a makeflow.
OPTIONS_END

SUBSECTION(Docker Support)
OPTIONS_BEGIN
OPTION_PAIR(--docker,image) Run each task in the Docker container with this name.  The image will be obtained via "docker pull" if it is not already available.
OPTION_PAIR(--docker-tar,tar) Run each task in the Docker container given by this tar file.  The image will be uploaded via "docker load" on each execution site.
OPTIONS_END

SUBSECTION(Singularity Support)
OPTIONS_BEGIN
OPTION_PAIR(--singularity,image) Run each task in the Singularity container with this name.  The container will be created from the passed in image.
OPTIONS_END



SUBSECTION(Amazon Options)
OPTIONS_BEGIN
OPTION_PAIR(--amazon-credentials,path) Specify path to Amazon credentials file.
The credentials file should be in the following JSON format:
LONGCODE_BEGIN
{
"aws_access_key_id" : "AAABBBBCCCCDDD"
"aws_secret_access_key" : "AAABBBBCCCCDDDAAABBBBCCCCDDD"
}
LONGCODE_END
OPTION_PAIR(--amazon-ami, image-id) Specify an amazon machine image.
OPTIONS_END

SUBSECTION(Mesos Options)
OPTIONS_BEGIN
OPTION_PAIR(--mesos-master, hostname) Indicate the host name of preferred mesos master.
OPTION_PAIR(--mesos-path, filepath) Indicate the path to mesos python2 site-packages.
OPTION_PAIR(--mesos-preload, library) Indicate the linking libraries for running mesos..
OPTIONS_END

SUBSECTION(Mountfile Support)
OPTIONS_BEGIN
OPTION_PAIR(--mounts, mountfile)Use this file as a mountlist. Every line of a mountfile can be used to specify the source and target of each input dependency in the format of BOLD(target source) (Note there should be a space between target and source.).
OPTION_PAIR(--cache, cache_dir)Use this dir as the cache for file dependencies.
OPTIONS_END

SUBSECTION(Archiving Options)
OPTIONS_BEGIN
OPTION_PAIR(--archive,path)Archive results of workflow at the specified path (by default /tmp/makeflow.archive.$UID) and use outputs of any archived jobs instead of re-executing job
OPTION_PAIR(--archive-read,path)Only check to see if jobs have been cached and use outputs if it has been
OPTION_PAIR(--archive-write,path)Write only results of each job to the archiving directory at the specified path
OPTIONS_END

SUBSECTION(Other Options)
OPTIONS_BEGIN
OPTION_ITEM(`-A, --disable-afs-check')Disable the check for AFS. (experts only)
OPTION_ITEM(`-z, --zero-length-error')Force failure on zero-length output files.
OPTION_PAIR(--wrapper,script) Wrap all commands with this BOLD(script). Each rule's original recipe is appended to BOLD(script) or replaces the first occurrence of BOLD({}) in BOLD(script).
OPTION_PAIR(--wrapper-input,file) Wrapper command requires this input file. This option may be specified more than once, defining an array of inputs. Additionally, each job executing a recipe has a unique integer identifier that replaces occurrences BOLD(%%) in BOLD(file).
OPTION_PAIR(--wrapper-output,file) Wrapper command requires this output file. This option may be specified more than once, defining an array of outputs. Additionally, each job executing a recipe has a unique integer identifier that replaces occurrences BOLD(%%) in BOLD(file).
OPTION_ITEM(`--enforcement')Use Parrot to restrict access to the given inputs/outputs.
OPTION_PAIR(--parrot,path)Path to parrot_run executable on the host system.
OPTION_PAIR(--shared-fs,dir)Assume the given directory is a shared filesystem accessible at all execution sites.
OPTIONS_END

SECTION(DRYRUN MODE)

When the batch system is set to BOLD(-T) PARAM(dryrun), Makeflow runs as usual
but does not actually execute jobs or modify the system. This is useful to
check that wrappers and substitutions are applied as expected. In addition,
Makeflow will write an equivalent shell script to the batch system log
specified by BOLD(-L) PARAM(logfile). This script will run the commands in
serial that Makeflow would have run. This shell script format may be useful
for archival purposes, since it does not depend on Makeflow.

SECTION(ENVIRONMENT VARIABLES)

The following environment variables will affect the execution of your
BOLD(Makeflow):
SUBSECTION(BATCH_OPTIONS)

This corresponds to the BOLD(-B) PARAM(options) parameter and will pass extra
batch options to the underlying execution engine.

SUBSECTION(MAKEFLOW_MAX_LOCAL_JOBS)
This corresponds to the BOLD(-j) PARAM(#) parameter and will set the maximum
number of local batch jobs.  If a BOLD(-j) PARAM(#) parameter is specified, the
minimum of the argument and the environment variable is used.

SUBSECTION(MAKEFLOW_MAX_REMOTE_JOBS)
This corresponds to the BOLD(-J) PARAM(#) parameter and will set the maximum
number of local batch jobs.  If a BOLD(-J) PARAM(#) parameter is specified, the
minimum of the argument and the environment variable is used.
PARA
Note that variables defined in your BOLD(Makeflow) are exported to the
environment.

SUBSECTION(TCP_LOW_PORT)
Inclusive low port in range used with CODE(-p 0).

SUBSECTION(TCP_HIGH_PORT))
Inclusive high port in range used with CODE(-p 0).

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

Run makeflow locally with debugging:
LONGCODE_BEGIN
makeflow -d all Makeflow
LONGCODE_END

Run makeflow on Condor will special requirements:
LONGCODE_BEGIN
makeflow -T condor -B "requirements = MachineGroup == 'ccl'" Makeflow
LONGCODE_END

Run makeflow with WorkQueue using named workers:
LONGCODE_BEGIN
makeflow -T wq -a -N project.name Makeflow
LONGCODE_END

Create a directory containing all of the dependencies required to run the
specified makeflow
LONGCODE_BEGIN
makeflow -b bundle Makeflow
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_MAKEFLOW

FOOTER
