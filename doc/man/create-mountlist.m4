include(manual.h)dnl
HEADER(create_mountlist)dnl

SECTION(NAME)
BOLD(create_mountlist) - create the mountlist for redirecting the file access

SECTION(SYNOPSIS)
CODE(BOLD(create_mountlist [options]))

SECTION(DESCRIPTION)
If CODE(parrot_run) is used to repeat one experiment, one mountlist must be created so that the file access request of your program can be redirected into the package. CODE(create_mountlist) is used to create the mountlist.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ITEM(`-m, --mountfile')Set the path of the mountlist file.
OPTION_ITEM(`-p, --path')The path of the package.
OPTION_ITEM(`-h, --help')Show this help message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)
To create a mountlist for one package:
LONGCODE_BEGIN
% create_mountlist -p /tmp/package -m mountlist
LONGCODE_END
One file named mountlist will be generated after the successful execution of this commond. Then CODE(parrot_run) can be used to repeat the experiment.
LONGCODE_BEGIN
% parrot_run -m mountlist program [program_options]
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_PARROT

FOOTER

