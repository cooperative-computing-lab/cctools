include(manual.h)dnl
HEADER(program name)

SECTION(NAME)
BOLD(program) - one sentence explanation

SECTION(SYNOPSIS)
CODE(BOLD(program [options] arguments))

SECTION(DESCRIPTION)

Describe what the program is good for in one or two paragraphs here.
PARA
Here is how to split paragraphs.
PARA
Examples of type styles: BOLD(bold), ITALIC(italic), CODE(code).

SECTION(OPTIONS)

List all options here in alphabetical order.
It's ok to copy and paste from the program help text or the manual.

OPTIONS_BEGIN
OPTION_PAIR(-d,flag) Enable debugging for this sub-system.
OPTION_ITEM(-h) Display version information.
OPTION_ITEM(-v) Show help text.
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)
List any environment variables used or set in this section.

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

Give 1-3 short examples of how to use the program:

To access a single remote file using BOLD(vi):

LONGCODE_BEGIN
parrot_run vi /anonftp/ftp.gnu.org/pub/README
LONGCODE_END

You can also run an entire shell inside of Parrot, like this:

LONGCODE_BEGIN
parrot_run bash
cd /anonftp/ftp.gnu.org/pub
ls -la
cat README
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(Parrot User Manual,"http://www.nd.edu/~ccl/software/manuals/parrot.html")
LIST_ITEM MANPAGE(parrot_run,1)
LIST_ITEM MANPAGE(makeflow,1)
LIST_END

FOOTER

