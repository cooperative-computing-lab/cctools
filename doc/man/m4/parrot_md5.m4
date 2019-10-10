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
If the filesystem does not support the checksum function internally,
it is computed by the user-level program in the normal fashion.

SECTION(OPTIONS)
CODE(parrot_md5) has no options.

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To retrieve the BOLD(MD5) checksum of a file stored on a BOLD(chirp) server:

LONGCODE_BEGIN
% parrot_run parrot_md5 /chirp/server.nd.edu/joe/data
	d41d8cd98f00b204e9800998ecf8427e data
LONGCODE_END


SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_PARROT

FOOTER
