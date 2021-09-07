include(manual.h)dnl
HEADER(parrot_mount)

SECTION(NAME)
BOLD(parrot_mount) - mount new directories inside of a Parrot instance.

SECTION(SYNOPSIS)
CODE(parrot_mount PARAM(path) PARAM(destination) PARAM(permissions))

SECTION(DESCRIPTION)
CODE(parrot_mount) utilizes BOLD(Parrot) system calls to change the namespace
of the parrot filesystem while it is running.  New mount points can be
added with read, write, or execute permissions, and existing mount points
can be removed.  The namespace can be locked down with the CODE(--disable)
option, which prevents any further changes in the current session.

OPTIONS_BEGIN
OPTION_ARG_LONG(unmount,path) Unmount a previously mounted path.
OPTION_FLAG_LONG(disable) Disable any further mounting/unmounting in this parrot session.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

Mount a remote Chirp filesystem as a read-only data directory:

LONGCODE_BEGIN
% parrot_run bash
% parrot_mount /chirp/fs.somewhere.edu/data /data R
LONGCODE_END

Umount the same directory:

LONGCODE_BEGIN
parrot_mount --unmount /data
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_PARROT

FOOTER
