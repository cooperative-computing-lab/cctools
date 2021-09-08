include(manual.h)dnl
HEADER(makeflow_ec2_cleanup)

SECTION(NAME)
BOLD(makeflow_ec2_cleanup) - clean up Amazon services after running Makeflow

SECTION(SYNOPSIS)
CODE(makeflow_ec2_cleanup PARAM(config-file))

SECTION(DESCRIPTION)

CODE(makeflow_ec2_cleanup) cleans up the virtual private cluster,
subnets, and other details that were created by CODE(makeflow_ec2_setup)
and stored in the config file passed on the command line.

It is possible that cleanup may fail, if the cluster or other elements
are still in use, and display a message from AWS.  In this case, make
sure that you have terminated any workflows running in the cluster and
try again.  If cleanup still fails, you may need to use the AWS console
to view and delete the relevant items listed in the config file.

SECTION(OPTIONS)
None.

SECTION(EXAMPLES)

LONGCODE_BEGIN
makeflow_ec2_cleanup my.config ami-343a694f
makeflow -T amazon --amazon-config my.config example.makeflow
makeflow_ec2_cleanup my.config
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_MAKEFLOW

FOOTER
