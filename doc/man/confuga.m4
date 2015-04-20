include(manual.h)dnl
HEADER(Confuga)

SECTION(NAME)
BOLD(Confuga) - An active storage cluster file system.

SECTION(SYNOPSIS)
CODE(BOLD(chirp_server --jobs --root=<Confuga URI> [options]))

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
BOLD(confuga:///path/to/workspace?option1=value&option2=value). Passing Confuga
specific options is done through this URI. Confuga's options are documented
here, with examples at the end of this manual.

OPTIONS_BEGIN
OPTION_PAIR(auth,method)Enable this method for Head Node to Storage Node authentication.
OPTION_PAIR(concurrency,limit)Limits the number of concurrent jobs executed by the cluster. The default is 0 for limitless.
OPTION_PAIR(nodes,node-list)Sets the list of storage nodes to use for the cluster. May be specified directly as a list BOLD(`node:<node1,node2,...>') or as a file BOLD(`file:<node file>').
OPTION_PAIR(pull-threshold,bytes)Sets the threshold for pull transfers. The default is 128MB.
OPTION_PAIR(replication,type)Sets the replication mode for satisfying job dependencies. BOLD(type) may be BOLD(push-sync) or BOLD(push-async-N). The default is BOLD(push-async-1).
OPTION_PAIR(scheduler,type)Sets the scheduler used to assign jobs to storage nodes. The default is BOLD(fifo-0).
OPTIONS_END

SECTION(EXAMPLES)

Launch a head node with workspace BOLD(./confuga.root), replication mode of BOLD(push-async-1), and a storage node list in file BOLD(nodes.lst):

LONGCODE_BEGIN
chirp_server --jobs --root='confuga://./confuga.root/?replication=push-async-1&nodes=file:nodes.lst'
LONGCODE_END

Launch a head node with workspace BOLD(/tmp/confuga.root) using storage nodes BOLD(chirp://localhost:10000/) and BOLD(chirp://localhost:10001/):

LONGCODE_BEGIN
chirp_server --jobs --root='confuga:///tmp/confuga.root/?nodes=node:chirp://localhost:10000/,chirp://localhost:10001/'
LONGCODE_END

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
             &
# sleep for a time so catalog can receive storage node status
sleep 5
# start the Confuga head node
chirp_server --advertise=localhost \\
             --catalog-name=localhost \\
             --catalog-update=30s \\
             --debug=confuga \\
             --jobs \\
             --root='confuga://./confuga.root/?auth=unix&nodes=node:chirp://localhost:9001/,chirp://localhost:9002/'
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_CHIRP

FOOTER
