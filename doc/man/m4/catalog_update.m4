include(manual.h)dnl
HEADER(catalog_update)

SECTION(NAME)
BOLD(catalog_update) - send update to catalog server

SECTION(SYNOPSIS)
CODE(catalog_update [options] [name=value] ..)

SECTION(DESCRIPTION)

PARA
The CODE(catalog_update) tool allows users to manually send an update to a
catalog server via TCP or UDP.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_ARG(c, catalog, host)Send update to this catalog host.
OPTION_ARG(f, file, json-file) Send additional JSON attributes in this file.
OPTION_ARG(d, debug, flags) Enable debug flags.
OPTION_ARG(o, debug-file, file) Send debug output to this file.
OPTION_ARG_SHORT(v,version) Show software version.
OPTION_ARG_SHORT(h, help) Show all options.

PARA
The CODE(catalog_update) tool sends a custom message to the catalog
server in the from of a JSON object with various properties describing
the host.  By default, the CODE(catalog_update) tool includes the following
fields in the update:

LIST_BEGIN
LIST_ITEM(CODE(type)) This describes the node type (default is "node").
LIST_ITEM(CODE(version)) This is the version of CCTools.
LIST_ITEM(CODE(cpu)) This is CPU architecture of the machine.
LIST_ITEM(CODE(opsys)) This is operating system of the machine.
LIST_ITEM(CODE(opsysversion)) This is operating system version of the machine.
LIST_ITEM(CODE(load1)) This is 1-minute load of the machine.
LIST_ITEM(CODE(load5)) This is 5-minute load of the machine.
LIST_ITEM(CODE(load15)) This is 15-minute load of the machine.
LIST_ITEM(CODE(memory_total)) This is total amount of memory on the machine
LIST_ITEM(CODE(memory_avail)) This is amount of available memory on the machine
LIST_ITEM(CODE(cpus)) This is number of detected CPUs on the machine.
LIST_ITEM(CODE(uptime)) This how long the machine has been running.
LIST_ITEM(CODE(owner)) This is user who sent the update.
LIST_END
OPTIONS_END

The field CODE(name) is intended to give a human-readable name to a service or
application which accepts incoming connections at CODE(port).


SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM(CODE(CATALOG_HOST)) Hostname of catalog server (same as CODE(-c)).
LIST_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)
PARA

The following example sends an update to the catalog server located at
CODE(catalog.cse.nd.edu) with three custom fields.

LONGCODE_BEGIN
% cat > test.json << EOF
{
    "type" : "node",
    "has_java" : true,
    "mode" : 3
}
EOF
% catalog_update -c catalog.cse.nd.edu -f test.json
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_CATALOG

FOOTER
