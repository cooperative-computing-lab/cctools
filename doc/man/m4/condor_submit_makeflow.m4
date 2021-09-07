include(manual.h)dnl
HEADER(condor_submit_makeflow)

SECTION(NAME)
BOLD(condor_submit_makeflow) - submit workflow to HTCondor batch system

SECTION(SYNOPSIS)
CODE(condor_submit_makeflow [options] PARAM(workflow))

SECTION(DESCRIPTION)
CODE(condor_submit_makeflow) submits Makeflow itself as a batch job,
allowing HTCondor to monitor and control the job, so the user does
not need to remain logged in while the workflow runs.  All options
given to condor_submit_makeflow are passed along to the underlying Makeflow.

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLE)
LONGCODE_BEGIN
condor_submit_makeflow example.mf

Submitting example.mf as a background job to HTCondor...
Submitting job(s).
1 job(s) submitted to cluster 364.
Makeflow Output : makeflow.364.output
Makeflow Error  : makeflow.364.error
Condor Log      : makeflow.364.condorlog
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_MAKEFLOW

FOOTER
