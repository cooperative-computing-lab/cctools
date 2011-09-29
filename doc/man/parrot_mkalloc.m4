include(manual.h)dnl
HEADER(parrot_mkalloc)

SECTION(NAME)
BOLD(parrot_mkalloc) - create a directory with a space allocation

SECTION(SYNOPSIS)
CODE(BOLD(parrot_mkalloc PARAM(path) PARAM(size)))

SECTION(DESCRIPTION)

CODE(parrot_mkalloc) creates a new directory with a space allocation.
As the name suggests, the command only runs correctly inside of the
Parrot virtual file system, on file servers where space allocation is enabled.
PARA
The BOLD(path) argument gives the new directory to create, and
the BOLD(size) argument indicates how large the space allocation should be.
The latter may use metric units such as K, M, B, etc to indicate kilobytes,
megabytes, gigabytes, and so forth.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_ITEM(-h) Show help text.
OPTIONS_END


SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.
If the command is attempted on a filesystem that does not support
space allocation, it will give the following error:

LONGCODE_BEGIN
parrot_mkalloc: This filesystem does not support allocations.
LONGCODE_END

SECTION(EXAMPLES)

To create a space allocation of ten gigabytes on a Chirp server:

LONGCODE_BEGIN
% parrot_run bash
% cd /chirp/myserver.somewhere.edu
% parrot_mkalloc bigdir 10G
% exit
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(Parrot User Manual,"http://www.nd.edu/~ccl/software/manuals/parrot.html")
LIST_ITEM MANPAGE(parrot_run,1)
LIST_ITEM MANPAGE(parrot_lsalloc,1)
LIST_END
