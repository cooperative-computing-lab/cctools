include(manual.h)dnl
HEADER(parrot_locate)

SECTION(NAME)
BOLD(parrot_locate) - provides the true location of the data stored in a named file.

SECTION(SYNOPSIS)
CODE(BOLD(parrot_locate path))

SECTION(DESCRIPTION)

CODE(parrot_locate) utilises BOLD(parrot) system calls to identify where the data stored as the file
at CODE(PARAM(path)) is actually located.  For example, running CODE(parrot_locate) on a file stored in a
BOLD(chirp) multi-volume will return the server name and file path on the server where the data is.
Running it on a file stored in BOLD(hdfs) will return the list of chunk servers storing the file.
PARA
Note that CODE(parrot_locate) varies depending on the underlying system.  Most systems return output
in the form "CODE(PARAM(server):PARAM(real path))", but that output is not guaranteed.


SECTION(OPTIONS)

CODE(parrot_locate) has no options.


SECTION(ENVIRONMENT VARIABLES)
Environment variables required by CODE(parrot_locate) are system dependent.
Most systems do not use or require any.  Refer to the specific system's documentation
for more information.


SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To check the location of a file stored in chirp:

LONGCODE_BEGIN
% parrot_run parrot_locate /chirp/server.nd.edu/joe/data
	server.nd.edu:/chirp/server.nd.edu/joe/data
LONGCODE_END

or a file stored in a chirp multi-volume

LONGCODE_BEGIN
% parrot_run parrot_locate /multi/server.nd.edu@multivol/data
	datastore01.nd.edu:multivol/data/ttmtteotsznxewoj
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_PARROT
