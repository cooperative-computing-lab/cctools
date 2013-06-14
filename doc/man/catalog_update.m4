include(manual.h)dnl
HEADER(catalog_update)

SECTION(NAME)
BOLD(catalog_update) - send update to catalog server

SECTION(SYNOPSIS)
CODE(BOLD(catalog_update [options] [name=value] ..))

SECTION(DESCRIPTION)

PARA
The CODE(catalog_update) tool allows users to manually send an update to a
catalog server via a short UDP packet.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_TRIPLET(-c, catalog, host)Send update to this catalog host.

PARA
The CODE(catalog_update) tool also accepts a list of CODE(name)/CODE(value)
pairs that will be sent to the catalog server as field entries.

By default, the CODE(catalog_update) tool includes the following field entries
in its update packet:

LIST_BEGIN
LIST_ITEM()CODE(BOLD(type)) This describes the node type (default is "node").
LIST_ITEM()CODE(BOLD(version)) This is the version of CCTools.
LIST_ITEM()CODE(BOLD(cpu)) This is CPU architecture of the machine.
LIST_ITEM()CODE(BOLD(opsys)) This is operating system of the machine.
LIST_ITEM()CODE(BOLD(opsysversion)) This is operating system version of the machine.
LIST_ITEM()CODE(BOLD(load1)) This is 1-minute load of the machine.
LIST_ITEM()CODE(BOLD(load5)) This is 5-minute load of the machine.
LIST_ITEM()CODE(BOLD(load15)) This is 15-minute load of the machine.
LIST_ITEM()CODE(BOLD(memory_total)) This is total amount of memory on the machine
LIST_ITEM()CODE(BOLD(memory_avail)) This is amount of available memory on the machine
LIST_ITEM()CODE(BOLD(cpus)) This is number of detected CPUs on the machine.
LIST_ITEM()CODE(BOLD(uptime)) This how long the machine has been running.
LIST_ITEM()CODE(BOLD(owner)) This is user who sent the update.
LIST_END
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM()CODE(BOLD(CATALOG_HOST)) Hostname of catalog server (same as CODE(-c)).
LIST_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)
PARA

The following example sends an update to the catalog server located at
CODE(catalog.cse.nd.edu) with the fields CODE(type) set to "node", CODE(name) set
to "testnode00", and CODE(has_java) set to "yes".  These fields can be queried
from the catalog server by clients using CODE(chirp_status).

LONGCODE_BEGIN
catalog_update -c catalog.cse.nd.edu type=node name=testnode00 has_java=yes
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_CHIRP

FOOTER

