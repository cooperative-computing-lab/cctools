include(manual.h)dnl
HEADER(parrot_pack)dnl

SECTION(NAME)
BOLD(parrot_pack) - generate a package based on the accessed files

SECTION(SYNOPSIS)
CODE(BOLD(parrot_pack [options]))

SECTION(DESCRIPTION)
After recording the accessed files of one program with the help of the CODE(--name-list) parameter of CODE(parrot_run), CODE(parrot_pack) can generate a package containing all the accessed files.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ITEM(--name-list)The path of the namelist list.
OPTION_ITEM(--env-path)The path of the environment variable file.
OPTION_ITEM(--package-path)The path of the package.
OPTION_ITEM(`-d, --debug')Enable debugging for this sub-system. (PARROT_DEBUG_FLAGS)
OPTION_ITEM(`-o, --debug-file')Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal) (PARROT_DEBUG_FILE).
OPTION_ITEM(`-h, --help')Show the help info.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)
To generate the package corresponding to CODE(namelist) and CODE(envlist):
LONGCODE_BEGIN
% parrot_pack --name-list namelist --env-path envlist --package-path /tmp/package
LONGCODE_END
After executing this command, one package with the path of CODE(/tmp/package) will be generated.
PARA
Here is a short instruction about how to make use of CODE(parrot_pack), CODE(env_process), CODE(create_mountlist) and CODE(parrot_run)
to generate one package for your experiment and repeat your experiment within your package.
PARA
Step 1: Preserve environment variables.
When the execution environment of your program is fixed (this should be done before you
generate the package), preserve environment variables using CODE(env_process).
LONGCODE_BEGIN
% env_process -s bash -p envlist
LONGCODE_END
At the end of step 1, one file named CODE(envlist) will be generated.
PARA
Step 2: Run your program under CODE(parrot_run) and using CODE(--name-list) parameter of Parrot to
record the filename list.
LONGCODE_BEGIN
% parrot_run --name-list namelist ...
LONGCODE_END
At the end of step 2, one file named CODE(namelist) will be generated.
PARA
Step 3: Using CODE(parrot_pack) to generate a package.
LONGCODE_BEGIN
% parrot_pack --name-list namelist --env-path envlist --package-path /tmp/package
LONGCODE_END
At the end of step 3, one package with the path of CODE(/tmp/package) will be generated.
PARA
Step 4: After you find out a new environment to repeat your experiment, another workstation or
a Virtual Machine, first obtain the package and cctools.
PARA
Step 5: Create the mountlist file so that the file access request of your program can be redirected
into the package.
LONGCODE_BEGIN
% create_mountlist -p /tmp/package -m mountlist
LONGCODE_END
At the end of step 5, one file named CODE(mountlist) will be generated.
PARA
Step 6: Repeat your program within your package.
LONGCODE_BEGIN
% parrot_run -m mountlist ...
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_PARROT
FOOTER

