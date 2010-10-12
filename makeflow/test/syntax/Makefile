CCTOOLS_HOME = ../../../
MAKEFLOW = ../../src/makeflow

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

test: ${MAKEFLOW}
	@# Run sample ./Makeflow
	@${MAKEFLOW} >/dev/null && ${MAKEFLOW} -c >/dev/null
	@# Check syntax of the site's example makeflow
	@${MAKEFLOW} -C test.makeflow
	@# Execute the two meta-makeflows, checking for proper name translation
	@${MAKEFLOW} meta.makeflow &>/dev/null && ${MAKEFLOW} -c meta.makeflow &>/dev/null
	@${MAKEFLOW} meta2.makeflow &>/dev/null && ${MAKEFLOW} -c meta2.makeflow &>/dev/null
	@# Check syntax, then attempt typo.makeflow. Syntax is OK, but DAG is
	@# inconsistent due to the typo resulting in a broken dependency.
	@${MAKEFLOW} -C typo.makeflow
	@$(SHELL) -c 'if ${MAKEFLOW} typo.makeflow ; then exit 1; else exit 0; fi'
	@# Check syntax for long_line_test.makeflow: syntax OK, but there's no
	@# way the makeflow would actually run.
	@./long_line_test.pl > long_line_test.makeflow
	@${MAKEFLOW} -C long_line_test.makeflow
	@# Check syntax for variable_issues.makeflow: FAILS.
	@$(SHELL) -c 'if ${MAKEFLOW} -C variable_issues.makeflow ; then exit 1; else exit 0; fi'
