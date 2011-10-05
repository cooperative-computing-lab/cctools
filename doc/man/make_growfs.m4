include(manual.h)dnl
HEADER(make_growfs)

SECTION(NAME)
BOLD(make_growfs) - generate directory listings for the GROW filesystem

SECTION(SYNOPSIS)
CODE(BOLD(make_growfs [options] PARAM(directory)))

SECTION(DESCRIPTION)

BOLD(make_growfs) prepares a local filesystem to be exported as
a GROW filesystem which can be accessed by MANPAGE(parrot_run,1).
Given a directory as an argument, it recursively visits all of
the directories underneath and creates files named CODE(.__growfsdir)
that summarize the metadata of all files in that directory.
PARA
Once the directory files are generated, the files may be accessed
through a web server as if there were on a full-fledged filesystem
with complete metadata.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_ITEM(-v)Give verbose messages.
OPTION_ITEM(-K)Create checksums for files. (default)
OPTION_ITEM(-k)Disable checksums for files.
OPTION_ITEM(-f)Follow all symbolic links.
OPTION_ITEM(-F)Do not follow any symbolic links.
OPTION_ITEM(-a)Only follow links that fall outside the root.  (default)
OPTION_ITEM(-h)Show help text.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

Suppose that your university web server exports the
directory CODE(/home/fred/www) as CODE(http://www.somewhere.edu/fred).
To create a GROW filesystem, put whatever files and directories you
like into CODE(/home/fred/www).  Then, run the following to generate
the GROW data:

LONGCODE_BEGIN
% make_growfs /home/fred/www
LONGCODE_END

Now that the GROW data is generated, you can use MANPAGE(parrot_run,1)
to treat the web address as a read-only filesystem:

LONGCODE_BEGIN
% parrot_run bash
% cd /growfs/www.somewhere.edu/fred
% ls -la
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(Parrot User Manual,"http://www.nd.edu/~ccl/software/manuals/parrot.html")
LIST_ITEM MANPAGE(parrot_run,1)
LIST_END
