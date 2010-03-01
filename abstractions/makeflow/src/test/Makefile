CCTOOLS_HOME = ../../../../
MAKEFLOW = ../makeflow

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

test: ${MAKEFLOW}
	${MAKEFLOW} test.makeflow && ${MAKEFLOW} -c test.makeflow
	${MAKEFLOW} meta.makeflow && ${MAKEFLOW} -c meta.makeflow
	${MAKEFLOW} exec_dependencies.makeflow && ${MAKEFLOW} -c exec_dependencies.makeflow
	$(SHELL) -c 'if ${MAKEFLOW} typo.makeflow ; then exit 1; else exit 0; fi'
	$(SHELL) -c 'if ${MAKEFLOW} long_line_test.makeflow ; then exit 1; else exit 0; fi'
	$(SHELL) -c 'if ${MAKEFLOW} variable_issues.makeflow ; then exit 1; else exit 0; fi'
	echo "All tests have been run successfully."

