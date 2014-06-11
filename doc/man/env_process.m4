include(manual.h)dnl
HEADER(env_process)dnl

SECTION(NAME)
BOLD(env_process) - preserve the environment variables

SECTION(SYNOPSIS)
CODE(BOLD(env_process [options]))

SECTION(DESCRIPTION)
When the execution environment of your program is fixed (this should be done before you generate the package), preserve environment variables using CODE(env_process).

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ITEM(`-s, --shell')The type of shell used to do the experiment. Supported shell types: csh, tcsh, bash, zsh.
OPTION_ITEM(`-p, --path')File to write the environment variables and its values.
OPTION_ITEM(`-h, --help')Show this help message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)
To preserve the environment variables used for bash script:
LONGCODE_BEGIN
% env_process -s bash -p envlist
LONGCODE_END
One file named <b>envlist</b> will be created after this command is done.

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_PARROT

FOOTER

