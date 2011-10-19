include(manual.h)dnl
HEADER(parrot_whoami)

SECTION(NAME)
BOLD(parrot_whoami) - returns the user's credentials (id and authentication method) from the perspective of the system being accessed.

SECTION(SYNOPSIS)
CODE(BOLD(parrot_whoami PARAM(path)))

SECTION(DESCRIPTION)

CODE(parrot_whoami) interrogates the system being accessed at PARAM(path) and returns the user's id
from the perspective of that system as well as the authentication method being used.  The specific
results depend on the system being accessed.
PARA
If PARAM(path) is not provided the current working directory is used.

SECTION(OPTIONS)

CODE(parrot_whoami) has no options.

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To get the user's credentials when accessing a remote chirp server:
LONGCODE_BEGIN
% parrot_run parrot_whoami /chirp/server.nd.edu/joe_data/data
unix:joe
LONGCODE_END

If you're working within a remote directory, PARAM(path) is not necessary:
LONGCODE_BEGIN
% parrot_run tcsh
% cd /chirp/server.nd.edu/joe_data/data
% parrot_whoami
unix:joe
% exit
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_PARROT

FOOTER
