include(manual.h)dnl
HEADER(confuga_adm)

SECTION(NAME)
BOLD(Confuga Administration) - Administrating the Confuga cluster.

SECTION(SYNOPSIS)
CODE(BOLD(confuga_adm [options] <Confuga URI> <command> [command options] ...))

SECTION(DESCRIPTION)

PARA
Performs administrative commands on the Confuga cluster.

PARA
For complete details with examples, see the LINK(Confuga User's Manual,http://ccl.cse.nd.edu/software/manuals/confuga.html).

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_TRIPLET(-d, debug, flag)Enable debugging for this sybsystem
OPTION_ITEM(`-h, --help')Give help information.
OPTION_TRIPLET(-o,debug-file,file)Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
OPTION_ITEM(`-v, --version')Show version info.
OPTIONS_END

SECTION(COMMANDS)

LIST_BEGIN
LIST_ITEM(BOLD(sn-add [-r <root>] [-p <password file>] <"uuid"|"address"> <uuid|address>)) Add a storage node to the cluster. Using the UUID of the Chirp server is recommended.
LIST_ITEM(BOLD(sn-rm [options] <"uuid"|"address"> <uuid|address>)) Remove a storage from the cluster. Using the UUID of the Chirp server is recommended. The storage node is removed when Confuga no longer relies on it to maintain minimum replication for files and when the storage node completes all running jobs.
LIST_END

SECTION(EXAMPLES)

PARA
Add a storage node with Confuga state stored in BOLD(/u/joe/confuga):

LONGCODE_BEGIN
confuga_adm sn-add -r /u/joe/confuga address 10.0.1.125:9090
LONGCODE_END

PARA
Remove a storage node:

LONGCODE_BEGIN
confuga_adm sn-rm address 10.0.1.125:9090
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
MANPAGE(confuga,1)

FOOTER
