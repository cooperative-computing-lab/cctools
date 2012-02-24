include(manual.h)dnl
HEADER(parrot_lsalloc)

SECTION(NAME)
BOLD(parrot_lsalloc) - list current status of a space allocation

SECTION(SYNOPSIS)
CODE(BOLD(parrot_lsalloc [path]))

SECTION(DESCRIPTION)

CODE(parrot_lsalloc) examines a given directory, determines if it is
contained within a space allocation, and then displays the allocation
size and the current usage. As the name suggests, the command only runs
correctly inside of the Parrot virtual file system, on file servers where space allocation is enabled.
PARA
The BOLD(path) argument gives the directory to examine.
If none is given, the current directory is assumed.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_ITEM(-h) Show help text.
OPTIONS_END


SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.
If the command is attempted on a filesystem that does not support
space allocation, it will give the following error:

LONGCODE_BEGIN
parrot_lsalloc: This filesystem does not support allocations.
LONGCODE_END

SECTION(EXAMPLES)

To list a space allocation on a Chirp server:

LONGCODE_BEGIN
% parrot_run bash
% cd /chirp/myserver.somewhere.edu
% parrot_lsalloc bigdir
/chirp/myserver.somewhere.edu/bigdir
10 GB TOTAL
1 GB INUSE
9 GB AVAIL
% exit
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_PARROT

FOOTER

