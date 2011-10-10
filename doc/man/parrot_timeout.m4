include(manual.h)dnl
HEADER(parrot_timeout)

SECTION(NAME)
BOLD(parrot_timeout) - changes or resets the master timeout for the current BOLD(parrot) session

SECTION(SYNOPSIS)
CODE(BOLD(parrot_timeout PARAM(time)))

SECTION(DESCRIPTION)

CODE(parrot_timeout) changes the master timeout for the current BOLD(parrot) session to
PARAM(time).  If PARAM(time) was not given, it resets it to the default value (5 minutes if
an interactive session or 1 hour for a non-interactive session).

SECTION(OPTIONS)

CODE(parrot_timeout) has no options.

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To change the master timeout to 5 hours:

LONGCODE_BEGIN
% parrot_run tcsh
% parrot_timeout 5h
% ./my_executable
% exit
LONGCODE_END

To change it to 30 seconds for one program and then reset it to the default value
LONGCODE_BEGIN
% parrot_run tcsh
% parrot_timeout 40m
% ./my_executable
% parrot_timeout
% ./my_second_executable
% exit
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_PARROT
