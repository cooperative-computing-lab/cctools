include(manual.h)dnl
HEADER(linker)

SECTION(NAME)
BOLD(linker) - automatic dependency location for workflows

SECTION(SYNOPSIS)
CODE(BOLD(linker [options] PARAM(workflow_description)))

SECTION(DESCRIPTION)
BOLD(linker) is a tool for automatically determining dependencies of workflows. It accepts a workflow description, currently Makeflow syntax is required, and recursively determines the dependencies and produces a self-contained package. BOLD(linker) supports Python, Perl, and shared libraries. 

PARA

BOLD(linker) finds dependencies by static analysis. CODE(eval) and other dynamic code loading may obscure dependencies causing BOLD(linker) to miss some critical dependencies. Therefore it is recommended to avoid these techniques when desiging a workflow.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ITEM(`-h, --help')Show this help screen.
OPTION_TRIPLET(-o, output, directory)Specify output directory
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure (typically permission errors), returns non-zero.

SECTION(BUGS)
LIST_BEGIN
LIST_ITEM The linker does not check for naming collisions beyond the initial workflow inputs.
LIST_ITEM The linker relies on regex parsing of files, so some forms of import statements may be missing.
LIST_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_LINKER

FOOTER
