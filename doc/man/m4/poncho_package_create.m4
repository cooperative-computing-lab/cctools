include(manual.h)dnl
HEADER(poncho_package_create)

SECTION(NAME)
BOLD(poncho_package_create) - command-line utility for creating a Conda virtual environment given a Python dependencies file

SECTION(SYNOPSIS)

CODE(poncho_package_create [options] PARAM(dependency-file) PARAM(PARAM(output-path)))

SECTION(DESCRIPTION)

BOLD(poncho_package_create) is a simple command-line utility that creates a local Conda environment from an input JSON dependency file.
The command creates an environment tarball at output-path that can be sent to and run on different machines with the same architecture.

The CODE(dependency-file) argument is the path (relative or absolute) to the a JSON specification file. The CODE(output-path) argument specifies the path for the environment tarball that is created
(should usually end in .tar.gz).

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ARG_LONG(conda-executable, path) Location of conda executable to use. If not given, mamba, $CONDA_EXE, conda, and microconda are tried, in that order.
OPTION_FLAG_LONG(no-microconda) Do not try to download microconda if a conda executable is not found.
OPTION_FLAG(h, help)               Show the help message.
OPTIONS_END
SECTION(EXIT STATUS)

On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLE)

CODE(poncho_package_create dependencies.json example_venv.tar.gz)

This will create an example_venv.tar.gz environment tarball within the current working directory, which can then be exported to different machines for execution.

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

FOOTER
