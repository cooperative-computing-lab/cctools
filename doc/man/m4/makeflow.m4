include(manual.h)dnl
HEADER(makeflow)

SECTION(NAME)
BOLD(makeflow) - workflow engine for executing distributed workflows

SECTION(SYNOPSIS)
CODE(makeflow [options] PARAM(dagfile))

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
OPTION_ARG(c, clean, option)Clean up: remove logfile and all targets. If option is one of [intermediates, outputs, cache], only indicated files are removed.
OPTION_ARG(f, summary-log, file)Write summary of workflow to file.
OPTION_FLAG(h,help)Show this help screen.
OPTION_FLAG(v,version)Show version string.
OPTION_ARG(X, chdir, directory)Chdir to enable executing the Makefile in other directory.
OPTION_ARG_LONG(argv, file)Use command line arguments from JSON file.
OPTIONS_END

SUBSECTION(Workflow Handling)
OPTIONS_BEGIN
OPTION_FLAG(a,advertise)Advertise the manager information to a catalog server.
OPTION_ARG(l, makeflow-log, logfile)Use this file for the makeflow log. (default is X.makeflowlog)
OPTION_ARG(L, batch-log, logfile)Use this file for the batch system log. (default is X.PARAM(type)log)
OPTION_ARG(m, email, email)Email summary of workflow to address.
OPTION_ARG(j, max-local, #)Max number of local jobs to run at once. (default is # of cores)
OPTION_ARG(J, max-remote, #)Max number of remote jobs to run at once. (default is 1000 for -Twq, 100 otherwise)
OPTION_FLAG(R,retry)Automatically retry failed batch jobs up to 100 times.
OPTION_ARG(r, retry-count, n)Automatically retry failed batch jobs up to n times.
OPTION_ARG_LONG(local-cores, #)Max number of cores used for local execution.
OPTION_ARG_LONG(local-memory, #)Max amount of memory used for local execution.
OPTION_ARG_LONG(local-disk, #)Max amount of disk used for local execution.

OPTION_END

SUBSECTION(Batch Options)
OPTIONS_BEGIN
OPTION_ARG(B, batch-options, options)Add these options to all batch submit files.
OPTION_FLAG_LONG(send-environment)Send all local environment variables in remote execution.
OPTION_ARG_LONG(wait-for-files-upto, #)Wait for output files to be created upto this many seconds (e.g., to deal with NFS semantics).
OPTION_ARG(S, submission-timeout, timeout)Time to retry failed batch job submission. (default is 3600s)
OPTION_ARG(T, batch-type, type)Batch system type: local, dryrun, condor, sge, pbs, torque, blue_waters, slurm, moab, cluster, wq, amazon, mesos. (default is local)
OPTION_FLAG_LONG(safe-submit-mode)Excludes resources at submission. (SLURM, TORQUE, and PBS)
OPTION_FLAG_LONG(ignore-memory-spec)Excludes memory at submission. (SLURM)
OPTION_ARG_LONG(batch-mem-type, type)Specify memory resource type. (SGE)
OPTION_ARG_LONG(working-dir, dir|url)Working directory for batch system.
OPTION_FLAG_LONG(sandbox)Run task in sandbox using bash script and task directory.
OPTION_FLAG_LONG(verbose-jobnames)Set the job name based on the command.
OPTION_FLAG_LONG(keep-wrapper-stdout)Do not redirect to /dev/null the stdout file from the batch system.
OPTIONS_END

SUBSECTION(JSON/JX Options)
OPTIONS_BEGIN
OPTION_FLAG_LONG(json)Interpret PARAM(dagfile) as a JSON format Makeflow.
OPTION_FLAG_LONG(jx)Evaluate JX expressions in PARAM(dagfile). Implies --json.
OPTION_ARG_LONG(jx-args, args)Read variable definitions from the JX file PARAM(args).
OPTION_ARG_LONG(jx-define, VAL=EXPR)Set the variable PARAM(VAL) to the JX expression PARAM(EXPR).
OPTION_ARG_LONG(jx-context, ctx)Deprecated. See '--jx-args'.
OPTIONS_END

SUBSECTION(Debugging Options)
OPTIONS_BEGIN
OPTION_ARG(d, debug, subsystem)Enable debugging for this subsystem.
OPTION_ARG(o,debug-file,file)Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
OPTION_ARG_LONG(debug-rotate-max, byte)Rotate debug file once it reaches this size.
OPTION_FLAG_LONG(verbose)Display runtime progress on stdout.
OPTIONS_END

SUBSECTION(TaskVine and Work Queue Options)
OPTIONS_BEGIN
OPTION_ARG(C, catalog-server, catalog)Set catalog server to PARAM(catalog). Format: HOSTNAME:PORT
OPTION_ARG(F, wq-fast-abort, #)WorkQueue fast abort multiplier. (default is deactivated)
OPTION_ARG(M, project-name, project)Set the project name to PARAM(project).
OPTION_ARG(p, port, port)Port number to use with WorkQueue. (default is 9123, 0=arbitrary)
OPTION_ARG(Z, port-file, file)Select port at random and write it to this file.  (default is disabled)
OPTION_ARG(P, priority, integer)Priority. Higher the value, higher the priority.
OPTION_ARG(t, keepalive-timeout, #)Work Queue keepalive timeout (default: 30s)
OPTION_ARG(u, keepalive-interval, #)Work Queue keepalive interval (default: 120s)
OPTION_ARG(W, schedule, mode)WorkQueue scheduling algorithm. (time|files|fcfs)
OPTION_ARG_LONG(password, pwfile)Password file for authenticating workers.
OPTION_ARG_LONG(ssl_cert) Set the SSL certificate file for encrypting connection.
OPTION_ARG_LONG(ssl_key) Set the SSL certificate file for encrypting connection.
OPTION_FLAG_LONG(cache-mode) Control worker caching mode. (never|workflow|forever)
OPTION_ARG_LONG(preferred-connection,connection)Indicate preferred connection. Chose one of by_ip or by_hostname. (default is by_ip)
OPTIONS_END

SUBSECTION(Monitor Options)
OPTIONS_BEGIN
OPTION_ARG_LONG(monitor, dir)Enable the resource monitor, and write the monitor logs to PARAM(dir)
OPTION_ARG_LONG(monitor, dir)Enable the resource monitor, and write the monitor logs to PARAM(dir)
OPTION_ARG_LONG(monitor-exe, file)Specify resource monitor executable.
OPTION_FLAG_LONG(monitor-with-time-series)Enable monitor time series.                 (default is disabled)
OPTION_FLAG_LONG(monitor-with-opened-files)Enable monitoring of openened files.        (default is disabled)
OPTION_ARG_LONG(monitor-interval, n)Set monitor interval to PARAM(n) seconds. (default 1 second)
OPTION_ARG_LONG(monitor-log-fmt, fmt)Format for monitor logs. (default resource-rule-%06.6d, %d -> rule number)
OPTION_FLAG_LONG(monitor-measure-dir)Monitor measures the task's current execution directory size.
OPTION_ARG_LONG(allocation, waste,throughput)When monitoring is enabled, automatically assign resource allocations to tasks. Makeflow will try to minimize CODE(waste) or maximize CODE(throughput).
OPTIONS_END

SUBSECTION(Umbrella Options)
OPTIONS_BEGIN
OPTION_ARG_LONG(umbrella-binary, filepath)Umbrella binary for running every rule in a makeflow.
OPTION_ARG_LONG(umbrella-log-prefix, filepath)Umbrella log file prefix for running every rule in a makeflow. (default is PARAM(makefilename).umbrella.log)
OPTION_ARG_LONG(umbrella-log-prefix, filepath)Umbrella log file prefix for running every rule in a makeflow. (default is PARAM(makefilename).umbrella.log)
OPTION_ARG_LONG(umbrella-mode, mode)Umbrella execution mode for running every rule in a makeflow. (default is local)
OPTION_ARG_LONG(umbrella-spec, filepath)Umbrella spec for running every rule in a makeflow.
OPTIONS_END

SUBSECTION(Docker Support)
OPTIONS_BEGIN
OPTION_ARG_LONG(docker,image) Run each task in the Docker container with this name.  The image will be obtained via "docker pull" if it is not already available.
OPTION_ARG_LONG(docker-tar,tar) Run each task in the Docker container given by this tar file.  The image will be uploaded via "docker load" on each execution site.
OPTION_ARG_LONG(docker-opt,string)Specify options to be used in DSingularityocker execution.
OPTIONS_END

SUBSECTION(Singularity Support)
OPTIONS_BEGIN
OPTION_ARG_LONG(singularity,image) Run each task in the Singularity container with this name.  The container will be created from the passed in image.
OPTION_ARG_LONG(singularity-opt,string)Specify options to be used in Singularity execution.
OPTIONS_END

SUBSECTION(Amazon Options)
OPTIONS_BEGIN
OPTION_ARG_LONG(amazon-config,path) Path to Amazon EC2 configuration file generated by makeflow_ec2_setup.
OPTIONS_END

SUBSECTION(Amazon Lambda Options)
OPTIONS_BEGIN
OPTION_ARG_LONG(lambda-config,path) Path to Amazon Lambda configuration file generated by makeflow_lambda_setup.
OPTIONS_END

SUBSECTION(Amazon Batch Options)
OPTIONS_BEGIN
OPTION_ARG_LONG(amazon-batch-config,path) Path to Amazon Batch configuration file generated by makeflow_amazon_batch_setup.
OPTION_ARG_LONG(amazon-batch-img,img) Specify image used for Amazon Batch execution.
OPTIONS_END

SUBSECTION(Mesos Options)
OPTIONS_BEGIN
OPTION_ARG_LONG(mesos-master, hostname) Indicate the host name of preferred mesos manager.
OPTION_ARG_LONG(mesos-path, filepath) Indicate the path to mesos python2 site-packages.
OPTION_ARG_LONG(mesos-preload, library) Indicate the linking libraries for running mesos..

SUBSECTION(Kubernetes Options)
OPTIONS_BEGIN
OPTION_ARG_LONG(k8s-image, docker_image) Indicate the Docker image for running pods on Kubernetes cluster. 
OPTIONS_END

SUBSECTION(Mountfile Support)
OPTIONS_BEGIN
OPTION_ARG_LONG(mounts, mountfile)Use this file as a mountlist. Every line of a mountfile can be used to specify the source and target of each input dependency in the format of BOLD(target source) (Note there should be a space between target and source.).
OPTION_ARG_LONG(cache, cache_dir)Use this dir as the cache for file dependencies.
OPTIONS_END

SUBSECTION(Archiving Options)
OPTIONS_BEGIN
OPTION_ARG_LONG(archive)Archive results of workflow at the specified path (by default /tmp/makeflow.archive.$UID) and use outputs of any archived jobs instead of re-executing job
OPTION_ARG_LONG(archive-dir,path)Specify archive base directory.
OPTION_ARG_LONG(archive-read,path)Only check to see if jobs have been cached and use outputs if it has been
OPTION_ARG_LONG(archive-s3,s3_bucket)Base S3 Bucket name
OPTION_ARG_LONG(archive-s3-no-check,s3_bucket)Blind upload files to S3 bucket (No existence check in bucket).
OPTION_ARG_LONG(s3-hostname, s3_hostname)Base S3 hostname. Used for AWS S3.
OPTION_ARG_LONG(s3-keyid, s3_key_id)Access Key for cloud server. Used for AWS S3.
OPTION_ARG_LONG(s3-secretkey, secret_key)Secret Key for cloud server. Used for AWS S3.
OPTIONS_END

SUBSECTION(VC3 Builder Options)
OPTIONS_BEGIN
OPTION_FLAG_LONG(vc3-builder)Enable VC3 Builder
OPTION_ARG_LONG(vc3-exe, file)VC3 Builder executable location
OPTION_ARG_LONG(vc3-log, file)VC3 Builder log name
OPTION_ARG_LONG(vc3-options, string)VC3 Builder option string
OPTIONS_END

SUBSECTION(Other Options)
OPTIONS_BEGIN
OPTION_FLAG(A,disable-afs-check)Disable the check for AFS. (experts only)
OPTION_FLAG(z,zero-length-error)Force failure on zero-length output files.
OPTION_ARG(g, gc, type)Enable garbage collection. (ref_cnt|on_demand|all)
OPTION_ARG_LONG(gc-size, int)Set disk size to trigger GC. (on_demand only)
OPTION_ARG(G, gc-count, int)Set number of files to trigger GC. (ref_cnt only)
OPTION_ARG_LONG(wrapper,script) Wrap all commands with this BOLD(script). Each rule's original recipe is appended to BOLD(script) or replaces the first occurrence of BOLD({}) in BOLD(script).
OPTION_ARG_LONG(wrapper-input,file) Wrapper command requires this input file. This option may be specified more than once, defining an array of inputs. Additionally, each job executing a recipe has a unique integer identifier that replaces occurrences BOLD(%%) in BOLD(file).
OPTION_ARG_LONG(wrapper-output,file) Wrapper command requires this output file. This option may be specified more than once, defining an array of outputs. Additionally, each job executing a recipe has a unique integer identifier that replaces occurrences BOLD(%%) in BOLD(file).
OPTION_FLAG_LONG(enforcement)Use Parrot to restrict access to the given inputs/outputs.
OPTION_ARG_LONG(parrot-path,path)Path to parrot_run executable on the host system.
OPTION_ARG_LONG(env-replace-path,path)Path to env_replace executable on the host system.
OPTION_FLAG_LONG(skip-file-check)Do not check for file existence before running.
OPTION_FLAG_LONG(do-not-save-failed-output)Disable saving failed nodes to directory for later analysis.
OPTION_ARG_LONG(shared-fs,dir)Assume the given directory is a shared filesystem accessible at all execution sites.
OPTION_ARG(X, change-directory, dir)Change to PARAM(dir) prior to executing the workflow.
OPTION_ARG(X, change-directory, dir)Change to PARAM(dir) prior to executing the workflow.
OPTIONS_END

SUBSECTION(MPI Options)
OPTIONS_BEGIN
OPTION_ARG_LONG(mpi-cores, #)Number of cores each MPI worker uses.
OPTION_ARG_LONG(mpi-memory, #)Amount of memory each MPI worker uses.
OPTION_ARG_LONG(mpi-task-working-dir, path)Path to the MPI tasks working directory.
OPTIONS_END

SECTION(DRYRUN MODE)

When the batch system is set to BOLD(-T) PARAM(dryrun), Makeflow runs as usual
but does not actually execute jobs or modify the system. This is useful to
check that wrappers and substitutions are applied as expected. In addition,
Makeflow will write an equivalent shell script to the batch system log
specified by BOLD(-L) PARAM(logfile). This script will run the commands in
serial that Makeflow would have run. This shell script format may be useful
for archival purposes, since it does not depend on Makeflow.

SECTION(MPI MODE)

When cctools is built with the --with-mpi-path configuration, Makeflow can be ran as an MPI program.
To do so, run Makeflow as an argument to BOLD(mpirun)/BOLD(mpiexec) and set BOLD(-T) PARAM(mpi) as a Makeflow option.
When submitting mpi, request one process per core. Makeflow will count up how many processes each node given to MPI
has, and use that as the core count for the worker on that node. Makeflow will then share memory evenly amongst the cores
on the node, following the following equation BOLD(worker_memory) = (BOLD(total_memory) / BOLD(total_logical_cores)) * BOLD(num_cores_for_worker).
To override Makeflow sharing memory equally, or setting per-worker cores value, use CODE(--mpi-cores) and CODE(--mpi-memory).

Tasks can also have their own sandbox. To specify the directory for tasks to create their sandbox subdirectory in, use CODE(--mpi-task-working-dir).

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
makeflow -T wq -a -M project.name Makeflow
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
