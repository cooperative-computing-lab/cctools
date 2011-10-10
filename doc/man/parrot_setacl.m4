include(manual.h)dnl
HEADER(parrot_setacl)

SECTION(NAME)
BOLD(parrot_setacl) - set ACL information for Parrot filesystem

SECTION(SYNOPSIS)
CODE(BOLD(parrot_setacl PARAM(path) PARAM(subject) PARAM(rights)))

SECTION(DESCRIPTION)
CODE(parrot_setacl) utilizes BOLD(Parrot) system calls to set the access
control list (ACL) information for the directory specified by PARAM(path).  The
PARAM(subject) argument refers to the entity to authorize, while the
PARAM(rights) argument is one of the following: read, write, admin, none.
PARA
Note, this program only works if it is executed under MANPAGE(parrot_run,1) and if the
underlying filesystem supports ACLs.

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)
Set read and list permissions for subject "unix:user" on a BOLD(Chirp) directory:

LONGCODE_BEGIN
% parrot_run parrot_setacl /chirp/student00.cse.nd.edu/user unix:user rl
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_PARROT
