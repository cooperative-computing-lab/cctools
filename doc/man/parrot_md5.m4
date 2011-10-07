include(manual.h)dnl
HEADER(parrot_md5)

SECTION(NAME)
BOLD(parrot_md5) - returns the BOLD(MD5) checksum of a file, generated on the remote system if possible.

SECTION(SYNOPSIS)
CODE(BOLD(parrot_md5 PARAM(path)))

SECTION(DESCRIPTION)

CODE(parrot_md5) returns the BOLD(MD5) checksum of the file stored at PARAM(path).  If possible
it calls a native function of the remote system to get the checksum, without requiring the transfer
of the file's contents to the user's machine.

SECTION(OPTIONS)
CODE(parrot_md5) has no options.

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To retrieve the BOLD(MD5) checksum of a file stored on a BOLD(chirp) server:

LONGCODE_BEGIN
parrot_run parrot_md5 /chirp/server.nd.edu/joe/data
	d41d8cd98f00b204e9800998ecf8427e data
LONGCODE_END


SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(Parrot User Manual,"http://www.nd.edu/~ccl/software/manuals/parrot.html")
LIST_ITEM MANPAGE(parrot_md5,1)
LIST_ITEM MANPAGE(parrot_run,1)
LIST_END
