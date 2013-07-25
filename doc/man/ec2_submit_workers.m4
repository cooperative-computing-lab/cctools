include(manual.h)dnl
HEADER(ec2_submit_workers)dnl

SECTION(NAME)
BOLD(ec2_submit_workers) - submit and run work_queue_worker on the Amazon EC2 service.

SECTION(SYNOPSIS)
CODE(BOLD(ec2_submit_workers [options] PARAM(servername) PARAM(port) PARAM(EC2-key-name) PARAM(EC2-key-file) PARAM(num-workers)))

SECTION(DESCRIPTION)
CODE(ec2_submit_workers) submits and runs MANPAGE(work_queue_worker,1) on the Amazon EC2 service. 
It calls EC2-run-instances and EC2-describe-instances that are part of the EC2 API tools to 
create EC2 instances and run work_queue_worker on them. The number of EC2 instances created is
given by the BOLD(num-workers) argument since each instance runs one work_queue_worker.

The BOLD(servername) and BOLD(port) arguments specify the hostname and port number of the master 
for the work_queue_worker to connect. These two arguments become optional when the auto mode 
option is specified for work_queue_worker. The BOLD(EC2-key-name) argument specifies the name of the
key to use in authenticating to the EC2 service. The BOLD(EC2-key-file) gives the path of the file
containing the (private) key.  

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ITEM(-M, name)Name of the preferred master for worker.
OPTION_PAIR(-N, name)Same as -M (backwards compatibility).
OPTION_PAIR(-c, cores)Set the number of cores each worker should use (0=auto). (default=1) 
OPTION_PAIR(-C, catalog)Set catalog server to <catalog>. <catalog> format: HOSTNAME:PORT.
OPTION_PAIR(-t, seconds)Abort work_queue_worker after this amount of idle time (default=900s).
OPTION_PAIR(-d, subsystem)Enable debugging on worker for this subsystem (try -d all to start).
OPTION_PAIR(-w, size)Set TCP window size
OPTION_PAIR(-i, time)Set initial value for backoff interval when worker fails to connect to a master. (default=1s)
OPTION_PAIR(-b, time)Set maxmimum value for backoff interval when worker fails to connect to a master. (default=60s)
OPTION_PAIR(-z, size)Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=100MB)
OPTION_PAIR(-A, arch)Set architecture string for the worker to report to master instead of the value in uname.
OPTION_PAIR(-O, os)Set operating system string for the worker to report to master instead of the value in uname. 
OPTION_PAIR(-s, path)Set the location for creating the working directory of the worker.
OPTION_PAIR(-I, image_id)EC2 OS image ID. Default = ami-fa01f193 (Ubuntu 10.04 x86_64).
OPTION_PAIR(-Z, instance_size)EC2 instance size. Default = m1.large.
OPTION_PAIR(-p, parameters)EC2-run-instances parameters.
OPTION_ITEM(-h)Show help message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)
Submit 10 work_queue_worker instances to run on EC2 service using key_1 as the authentication key 
whose private key string is found in key_1.priv. Run work_queue_worker in auto mode with the
preferred project name set to Project_A and abort timeout set to 3600 seconds:
LONGCODE_BEGIN
ec2_submit_workers -a -t 3600 -M Project_A key_1 key_1.priv 10
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_WORK_QUEUE

FOOTER

