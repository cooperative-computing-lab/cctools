include(manual.h)dnl
HEADER(makeflow_linker)

SECTION(NAME)
BOLD(makeflow_linker) - automatic dependency location for workflows

SECTION(SYNOPSIS)
CODE(makeflow_linker [options] PARAM(workflow_description))

SECTION(DESCRIPTION)
BOLD(makeflow_linker) is a tool for automatically determining dependencies of workflows. It accepts a workflow description, currently Makeflow syntax is required, and recursively determines the dependencies and produces a self-contained package. BOLD(makeflow_linker) supports Python, Perl, and shared libraries.

PARA

BOLD(makeflow_linker) finds dependencies by static analysis. CODE(eval) and other dynamic code loading may obscure dependencies causing BOLD(makeflow_linker) to miss some critical dependencies. Therefore it is recommended to avoid these techniques when desiging a workflow.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_FLAG_LONG(dry-run)Run without creating directories or copying dependencies.
OPTION_FLAG(h,help)Show this help screen.
OPTION_FLAG(n,use-named)Do not copy files which are part of a named dependency, e.g. standard libraries.
OPTION_ARG(o, output, directory)Specify output directory.
OPTION_FLAG_LONG(verbose)Output status during run.
OPTION_FLAG(v,version)Display version information.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure (typically permission errors), returns non-zero.

SECTION(BUGS)
LIST_BEGIN
LIST_ITEM(The makeflow_linker does not check for naming collisions beyond the initial workflow inputs.)
LIST_ITEM(The makeflow_linker relies on regex parsing of files, so some forms of import statements may be missing.)
LIST_END

SECTION(EXAMPLES)

Package a workflow:
LONGCODE_BEGIN
makeflow_linker -o example_mf example.mf
LONGCODE_END

Run packaged workflow:
LONGCODE_BEGIN
makeflow example_mf/example.mf
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_LINKER

FOOTER
