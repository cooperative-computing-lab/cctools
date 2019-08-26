include(manual.h)dnl
HEADER(makeflow_amazon_batch_cleanup)

SECTION(NAME)
BOLD(makeflow_amazon_batch_cleanup) - clean up Amazon services after running Makeflow

SECTION(SYNOPSIS)
CODE(BOLD(makeflow_amazon_batch_cleanup PARAM(config-file)))

SECTION(DESCRIPTION)

CODE(makeflow_amazon_batch_cleanup) cleans up the virtual private cluster,
subnets, and other details that were created by CODE(makeflow_amazon_batch_setup)
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
makeflow_amazon_batch_setup 3 2 4 my.config
makeflow -T amazon-batch --amazon-batch-config=my.config --amazon-ami=USER_PROVIDED_ECS_IMAGE_ARN example.makeflow
makeflow_amazon_batch_cleanup my.config
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_MAKEFLOW

FOOTER
