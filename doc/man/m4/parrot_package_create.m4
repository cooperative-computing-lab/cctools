include(manual.h)dnl
HEADER(parrot_package_create)

SECTION(NAME)
BOLD(parrot_package_create) - generate a package based on the accessed files and the preserved environment variables

SECTION(SYNOPSIS)
CODE(parrot_package_create [options])

SECTION(DESCRIPTION)
After recording the accessed files and environment variables of one program with the help of the CODE(--name-list) parameter and the CODE(--env-list) of CODE(parrot_run), CODE(parrot_package_create) can generate a package containing all the accessed files. You can also add the dependencies recorded in a new namelist file into an existing package.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ARG(a, add, path)The path of an existing package.
OPTION_ARG(e, env-list, path)The path of the environment variables.
OPTION_ARG_LONG(new-env, path)The relative path of the environment variable file under the package.
OPTION_ARG(n, name-list, path)The path of the namelist list.
OPTION_ARG(p, package-path, path)The path of the package.
OPTION_ARG(d, debug, flag)Enable debugging for this sub-system.
OPTION_ARG(o,debug-file,file)Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
OPTION_FLAG(h,help)Show the help info.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)
To generate the package corresponding to BOLD(namelist) and BOLD(envlist):
LONGCODE_BEGIN
% parrot_package_create --name-list namelist --env-list envlist --package-path /tmp/package
LONGCODE_END
After executing this command, one package with the path of BOLD(/tmp/package) will be generated.
PARA

Here is a short instruction about how to make use of CODE(parrot_run), CODE(parrot_package_create) and CODE(parrot_package_run)
to generate one package for your experiment and repeat your experiment within your package.
PARA
Step 1: Run your program under CODE(parrot_run) and using BOLD(--name-list) and BOLD(--env-list) parameters to
record the filename list and environment variables.
LONGCODE_BEGIN
% parrot_run --name-list namelist --env-list envlist /bin/bash
LONGCODE_END
After the execution of this command, you can run your program inside CODE(parrot_run). At the end of step 1, one file named BOLD(namelist) containing all the accessed file names and one file named BOLD(envlist) containing environment variables will be generated.
After everything is done, exit CODE(parrot_run):
LONGCODE_BEGIN
% exit
LONGCODE_END
PARA
Step 2: Using CODE(parrot_package_create) to generate a package.
LONGCODE_BEGIN
% parrot_package_create --name-list namelist --env-path envlist --package-path /tmp/package
LONGCODE_END
At the end of step 2, one package with the path of BOLD(/tmp/package) will be generated.
PARA
Step 3: Repeat your program within your package.
LONGCODE_BEGIN
% parrot_package_run --package-path /tmp/package --shell-type bash ...
LONGCODE_END
After the execution of this command, one shell will be returned, where you can repeat your original program (Please replace BOLD(--shell-type) parameter with the shell type you actually used). After everything is done, exit CODE(parrot_package_run):
LONGCODE_BEGIN
% exit
LONGCODE_END

You can also add the dependencies recorded in a new namelist file, BOLD(namelist1), into an existing package:
LONGCODE_BEGIN
% parrot_package_create --name-list namelist1 --env-list envlist1 --new-env envlist1  --add /tmp/package
LONGCODE_END
After executing this command, all the new dependencies mentioned in BOLD(namelist1) will be added into BOLD(/tmp/package), the new envlist, BOLD(envlist1), will also be added into BOLD(/tmp/package) with the name specified by the BOLD(--new-env) option.

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_PARROT
FOOTER
