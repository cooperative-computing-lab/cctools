CCTOOLS_HOME = ../../../../
MAKEFLOW = ../makeflow

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

test: ${MAKEFLOW}
	${MAKEFLOW} test.makeflow && exit 0
	${MAKEFLOW} -c test.makeflow && exit 0
	${MAKEFLOW} typo.makeflow || exit 0
	${MAKEFLOW} exec_dependencies.makeflow && exit 0
	${MAKEFLOW} -c exec_dependencies.makeflow && exit 0
	${MAKEFLOW} long_line_test.makeflow || exit 0
	${MAKEFLOW} variable_issues.makeflow || exit 0
	${MAKEFLOW} meta.makeflow && exit 0
	${MAKEFLOW} -c meta.makeflow && exit 0
	echo "All tests have been run successfully."

