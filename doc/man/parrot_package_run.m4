include(manual.h)dnl
HEADER(parrot_package_run)dnl

SECTION(NAME)
BOLD(parrot_package_run) - repeat a program with the package

SECTION(SYNOPSIS)
CODE(BOLD(parrot_package_run [options]))

SECTION(DESCRIPTION)
If CODE(parrot_run) is used to repeat one experiment, one mountlist must be created so that the file access request of your program can be redirected into the package. CODE(parrot_package_run) is used to create the mountlist.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ITEM(`-s, --shell-type')The type of shell used to do the experiment. (e.g., bash, tcsh, csh, zsh)
OPTION_ITEM(`-p, --package-path')The path of the package.
OPTION_ITEM(`-h, --help')Show this help message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)
To repeat one program within one package BOLD(/tmp/package) in a BOLD(bash) shell:
LONGCODE_BEGIN
% parrot_package_run --package-path /tmp/package --shell-type bash
LONGCODE_END
After the execution of this command, one shell will be returned, where you can repeat your original program. After everything is done, exit CODE(parrot_package_run):
LONGCODE_BEGIN
% exit
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_PARROT

FOOTER

