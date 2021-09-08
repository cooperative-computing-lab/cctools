include(manual.h)dnl
HEADER(poncho_package_run)

SECTION(NAME)

BOLD(poncho_package_run) - wrapper script that executes Python script within an isolated Conda environment

SECTION(SYNOPSIS)

CODE(poncho_package_run [options] --environment PARAM(file) PARAM(command and args ...))

SECTION(DESCRIPTION)

The BOLD(poncho_package_run) tool acts as a wrapper script for a Python task, running the task within the specified Conda environment. BOLD(poncho_package_run) can be utilized on different machines within the Work Queue system to unpack and activate a Conda environment, and run a task within the isolated environment.

The --environment PARAM(file) argument is the name of the Conda environment as a tarball file in which to run the Python task.
The --environment PARAM(file) argument is the name of the Conda environment as a tarball file in which to run the Python task.
command and args (the COMMAND) are interpreted as ARGV for a command to be run inside the Conda environment.

By default, the conda environment is unpacked into a temporary directory which is removed at the end of execution. If the --unpack-to PARAM(dir) is given, then the environment is unpacked to PARAM(dir), and it is not removed at the end of execution. Further (even simultaneous) executions of python_package_run will not unpack the environment if PARAM(dir) is already populated. Instances of python_package_run coordinate via a writing lock. By default, the wait for a writing lock is 300 seconds, but this can be modified with the --wait-for-lock PARAM(secs) option.

If the argument to --unpack-to does not exist, then it is created as an empty directory. If it is an existing directory, but it is not empty, then unpacking is not performed, regardless on whether this directory contains a valid conda environment.


SECTION(OPTIONS) 

OPTIONS_BEGIN
OPTION_ARG(e, environment, file)   Conda environment as a tar file. (Required.)
OPTION_ARG(d, unpack-to, dir)      Directory to unpack the environment. If not given, a temporary directory is used.
OPTION_ARG(w, wait-for-lock, secs) Number of seconds to wait to get a writing lock on PARAM(dir). Default is 300.
OPTION_ARG(w, wait-for-lock, secs) Number of seconds to wait to get a writing lock on PARAM(dir). Default is 300.
OPTION_FLAG(h, help)                Show the help screen.
OPTIONS_END
SECTION(EXIT STATUS)

On success, returns 0. On failure, returns non-zero.

SECTION(EXAMPLE)

CODE(poncho_package_run --environment example_venv.tar.gz python3 example.py)

This will run the command python3 example.py within the Conda environment in example_venv.tar.gz. Note that this command can be performed either locally, on the same machine that analyzed the script and created the environment, or remotely, on a different machine that contains the Conda environment tarball and the example.py script.

CODE(poncho_package_run --unpack-to my_persistent_env --environment example_venv.tar.gz python3 example.py)

The previous command will run faster the second time it is executed, as the environment is only unpacked once to my_persistent_env.


SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

FOOTER
