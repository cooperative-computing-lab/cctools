include(manual.h)dnl
HEADER(parrot_getacl)

SECTION(NAME)
BOLD(parrot_getacl) - get ACL information for Parrot filesystem

SECTION(SYNOPSIS)
CODE(BOLD(parrot_getacl PARAM(path)))

SECTION(DESCRIPTION)
CODE(parrot_getacl) utilizes BOLD(Parrot) system calls to retrieve the access
control list (ACL) information for the filesystem located at the specified
PARAM(path).
PARA
Note, this program only works if it is executed under BOLD(Parrot) and if the
underlying filesystem supports ACLs.

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)
Get ACL for BOLD(Chirp) directory:

LONGCODE_BEGIN
% parrot_run parrot_getacl /chirp/student00.cse.nd.edu/
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_PARROT
