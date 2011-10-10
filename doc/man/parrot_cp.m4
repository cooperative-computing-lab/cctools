include(manual.h)dnl
HEADER(parrot_cp)

SECTION(NAME)
BOLD(parrot_cp) - a replacement for CODE(cp) that provides higher performance when dealing
with remote files via CODE(parrot_run).

SECTION(SYNOPSIS)
CODE(BOLD(parrot_cp [options] ... sources ... PARAM(dest)))

SECTION(DESCRIPTION)

CODE(parrot_cp) is a drop-in replacement for the Unix CODE(cp) command.
It provides better performance when copying files to or from remote storage
systems by taking advantage of whole-file transfer rather than copying files
block-by-block.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_ITEM(-f) Forcibly remove target before copying.
OPTION_ITEM(-i) Interactive mode: ask before overwriting.
OPTION_ITEM(-r)
OPTION_ITEM(-R) Recursively copy directories.
OPTION_ITEM(-s) Make symbolic links instead of copying files.
OPTION_ITEM(-l) Make hard links instead of copying files.
OPTION_ITEM(-u) Update mode: Copy only if source is newer than target.
OPTION_ITEM(-v) Verbose mode: Show names of files copied.
OPTION_ITEM(-h) Help: Show these options.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To use parrot_cp you can either call the code directly:

LONGCODE_BEGIN
% parrot_run tcsh
% parrot_cp /tmp/mydata /chirp/server.nd.edu/joe/data
% exit
LONGCODE_END

or alias calls to CODE(cp) with calls to CODE(parrot_cp):

LONGCODE_BEGIN
% parrot_run bash
% alias cp parrot_cp
% cp -r /chirp/server.nd.edu/joe /tmp/joe
% exit
LONGCODE_END


SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_PARROT
