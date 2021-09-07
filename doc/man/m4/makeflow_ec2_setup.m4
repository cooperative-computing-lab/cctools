include(manual.h)dnl
HEADER(makeflow_ec2_setup)

SECTION(NAME)
BOLD(makeflow_ec2_setup) - set up Amazon services for running Makeflow

SECTION(SYNOPSIS)
CODE(makeflow_ec2_setup PARAM(config-file) PARAM(ami))

SECTION(DESCRIPTION)

Before using CODE(makeflow_ec2_setup), make sure that you install the AWS Command
Line Interface tools, have the CODE(aws) command in your PATH,
and run CODE(aws configure) to set up your secret keys and default region.

BOLD(makeflow_ec2_setup) prepares Amazon services for running a Makeflow.
It creates a virtual private cluster, subnets, key pairs, and other
details, and writes these out to a configuration file given by the 
first argument.  The second argument is the Amazon Machine Image (AMI)
that will be used by default in the workflow, if not specified for 
individual jobs.

Once the configuration file is created, you may run CODE(makeflow)
with the CODE(-T amazon) option and give the name of the created
config file with CODE(--amazon-config).

When complete, you can clean up the virtual cluster and related
items by running CODE(makeflow_ec2_cleanup)

SECTION(OPTIONS)
None.

SECTION(EXAMPLES)

LONGCODE_BEGIN
makeflow_ec2_setup my.config ami-343a694f
makeflow -T amazon --amazon-config my.config example.makeflow
makeflow_ec2_cleanup my.config
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_MAKEFLOW

FOOTER
