include(manual.h)dnl
HEADER(confuga)

SECTION(NAME)
BOLD(Confuga) - An active storage cluster file system.

SECTION(SYNOPSIS)
CODE(chirp_server --jobs --root=PARAM(Confuga URI) [options])

SECTION(DESCRIPTION)

PARA
Configures and starts a Chirp server to act as the head node for a Confuga
storage cluster.

PARA
For complete details with examples, see the LINK(Confuga User's Manual,http://ccl.cse.nd.edu/software/manuals/confuga.html).

SECTION(OPTIONS)

PARA
A Chirp server acting as the Confuga head node uses normal
MANPAGE(chirp_server,1) options. In order to run the Chirp server as the
Confuga head node, use the BOLD(--root) switch with the Confuga URI. You must
also enable job execution with the BOLD(--jobs) switch.

PARA
The format for the Confuga URI is:
BOLD(confuga:///path/to/workspace?option1=value&option2=value). The workspace
path is the location Confuga maintains metadata and databases for the head
node. Confuga specific options are also passed through the URI, documented
below.  Examples demonstrating how to start Confuga and a small cluster are at
the end of this manual.

OPTIONS_BEGIN
OPTION_ARG_LONG(auth,method)Enable this method for Head Node to Storage Node authentication. The default is to enable all available authentication mechanisms.
OPTION_ARG_LONG(concurrency,limit)Limits the number of concurrent jobs executed by the cluster. The default is 0 for limitless.
OPTION_ARG_LONG(pull-threshold,bytes)Sets the threshold for pull transfers. The default is 128MB.
OPTION_ARG_LONG(replication,type)Sets the replication mode for satisfying job dependencies. BOLD(type) may be BOLD(push-sync) or BOLD(push-async-N). The default is BOLD(push-async-1).
OPTION_ARG_LONG(scheduler,type)Sets the scheduler used to assign jobs to storage nodes. The default is BOLD(fifo-0).
OPTION_ARG_LONG(tickets,tickets)Sets tickets to use for authenticating with storage nodes. Paths must be absolute.
OPTIONS_END

SECTION(STORAGE NODES)

PARA
Confuga uses regular Chirp servers as storage nodes. Each storage node is
added to the cluster using the MANPAGE(confuga_adm,1) command.  All storage
node Chirp servers must be run with:

LIST_BEGIN
LIST_ITEM(Ticket authentication enabled (BOLD(--auth=ticket)). Remember by default all authentication mechanisms are enabled.)
LIST_ITEM(Job execution enabled (BOLD(--jobs)).)
LIST_ITEM(Job concurrency of at least two (BOLD(--job-concurrency=2)).)
LIST_END

PARA
These options are also suggested but not required:

LIST_BEGIN
LIST_ITEM(More frequent Catalog updates (BOLD(--catalog-update=30s)).)
LIST_ITEM(Project name for the cluster (BOLD(--project-name=foo)).)
LIST_END

PARA
You must also ensure that the storage nodes and the Confuga head node are using
the same MANPAGE(catalog_server,1). By default, this should be the case. The
BOLD(EXAMPLES) section below includes an example cluster using a manually
hosted catalog server.

SUBSECTION(ADDING STORAGE NODES)

PARA
To add storage nodes to the Confuga cluster, use the MANPAGE(confuga_adm,1)
administrative tool.

SECTION(EXECUTING WORKFLOWS)

PARA
The easiest way to execute workflows on Confuga is through MANPAGE(makeflow,1).
Only two options to Makeflow are required, BOLD(--batch-type) and
BOLD(--working-dir). Confuga uses the Chirp job protocol, so the batch type is
BOLD(chirp). It is also necessary to define the executing server, the Confuga
Head Node, and the ITALIC(namespace) the workflow executes in. For example:

LONGCODE_BEGIN
makeflow --batch-type=chirp --working-dir=chirp://confuga.example.com:9094/BOLD(path/to/workflow)
LONGCODE_END

PARA
The workflow namespace is logically prepended to all file paths defined in the
Makeflow specification. So for example, if you have this Makeflow file:

LONGCODE_BEGIN
a: exe
    ./exe > a
LONGCODE_END

PARA
Confuga will execute BOLD(/path/to/workflow/exe) and produce the output file BOLD(/path/to/workflow/a).

PARA
Unlike other batch systems used with Makeflow, like Condor or Work Queue,
ITALIC(all files used by a workflow must be in the Confuga file system). Condor
and Work Queue both stage workflow files from the submission site to the
execution sites. In Confuga, the entire workflow dataset, including
executables, is already resident.  So when executing a new workflow, you need
to upload the workflow dataset to Confuga. The easiest way to do this is using
the MANPAGE(chirp,1) command line tool:

LONGCODE_BEGIN
chirp confuga.example.com put workflow/ /path/to/
LONGCODE_END

PARA
Finally, Confuga does not save the ITALIC(stdout) or ITALIC(stderr) of jobs.
If you want these files for debugging purposes, you must explicitly save them.
To streamline the process, you may use Makeflow's BOLD(--wrapper) options to
save ITALIC(stdout) and ITALIC(stderr):

LONGCODE_BEGIN
makeflow --batch-type=chirp \\
         --working-dir=chirp://confuga.example.com/ \\
         --wrapper=$'{\\n{}\\n} > stdout.%% 2> stderr.%%' \\
         --wrapper-output='stdout.%%' \\
         --wrapper-output='stderr.%%'
LONGCODE_END

SECTION(EXAMPLES)

PARA
Launch a head node with Confuga state stored in BOLD(./confuga.root):

LONGCODE_BEGIN
chirp_server --jobs --root="confuga://$(pwd)/confuga.root/"
LONGCODE_END

PARA
Launch a head node with workspace BOLD(/tmp/confuga.root) using storage nodes BOLD(chirp://localhost:10001) and BOLD(chirp://localhost:10002/u/joe/confuga):

LONGCODE_BEGIN
chirp_server --jobs --root='confuga:///tmp/confuga.root/'
confuga_adm confuga:///tmp/confuga.root/ sn-add address localhost:10001
confuga_adm confuga:///tmp/confuga.root/ sn-add -r /u/joe/confuga address localhost:10001
LONGCODE_END

PARA
Run a simple test cluster on your workstation:

LONGCODE_BEGIN
# start a catalog server in the background
catalog_server --history=catalog.history \\
               --update-log=catalog.update \\
               --interface=127.0.0.1 \\
               &
# sleep for a time so catalog can start
sleep 1
# start storage node 1 in the background
chirp_server --advertise=localhost \\
             --catalog-name=localhost \\
             --catalog-update=10s \\
             --interface=127.0.0.1 \\
             --jobs \\
             --job-concurrency=10 \\
             --root=./root.1 \\
             --port=9001 \\
             --project-name=test \\
             --transient=./tran.1 \\
             &
# start storage node 2 in the background
chirp_server --advertise=localhost \\
             --catalog-name=localhost \\
             --catalog-update=10s \\
             --interface=127.0.0.1 \\
             --jobs \\
             --job-concurrency=10 \\
             --root=./root.2 \\
             --port=9002 \\
             --project-name=test \\
             --transient=./tran.2 \\
             &
# sleep for a time so catalog can receive storage node status
sleep 5
confuga_adm confuga:///$(pwd)/confuga.root/ sn-add address localhost:9001
confuga_adm confuga:///$(pwd)/confuga.root/ sn-add address localhost:9002
# start the Confuga head node
chirp_server --advertise=localhost \\
             --catalog-name=localhost \\
             --catalog-update=30s \\
             --debug=confuga \\
             --jobs \\
             --root="confuga://$(pwd)/confuga.root/?auth=unix" \\
             --port=9000
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
MANPAGE(confuga_adm,1) SEE_ALSO_CHIRP

FOOTER
