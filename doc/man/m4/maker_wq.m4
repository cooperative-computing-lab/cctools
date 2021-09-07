include(manual.h)dnl
HEADER(maker_wq)

SECTION(NAME)
BOLD(maker_wq) - Run the Maker genome annotation tool using Work Queue to harness heterogenous resources

SECTION(SYNOPSIS)
CODE(maker_wq [options] maker_opts maker_bopts maker_exe )

SECTION(DESCRIPTION)
BOLD(maker_wq) is a manager script to run the Maker genome annotation tool using Work Queue to enable the user to harness the heterogenous power of multiple systems simultaneously. It accepts all of the Maker inputs. The primary difference is that the MPI code has been replaced with Work Queue components.
PARA
BOLD(maker_wq) expects a maker_wq_worker in the path, and can be used from any working directory. All required input files are specified in the standard Maker control files just as in the standard Maker distribution.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ARG_SHORT(port, port)Specify the port on which to create the Work Queue
OPTION_ARG_SHORT(fa, fast_abort)Specify a fast abort multiplier
OPTION_ARG_SHORT(M, project)Specify a project name for utilizing shared workers
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(ENVIRONMENT VARIABLES)

SECTION(EXAMPLES)

To run maker_wq, specify the same arguments as standard Maker:
LONGCODE_BEGIN
maker_wq maker_opts.ctl maker_bopts.ctl maker_exe.ctl > output
LONGCODE_END
This will begin the Maker run. All that is needed now is to submit workers that can be accessed by our manager.

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

FOOTER
