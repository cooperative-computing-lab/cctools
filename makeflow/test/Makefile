CCTOOLS_HOME = ../..

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

all:

test-makeflow:
	@echo running makeflow tests
	@${CCTOOLS_HOME}/dttools/src/test_runner.sh TR_makeflow*.sh
	@if [ x$$CCTOOLS_TEST_ND = x1 ]; then ${CCTOOLS_HOME}/dttools/src/test_runner.sh ND_*.sh; fi

test-starch:
	@echo running starch tests
	@${CCTOOLS_HOME}/dttools/src/test_runner.sh TR_starch*.sh

test-nd: test
	@echo running notre dame makeflow tests

test: test-makeflow test-starch

clean:
	-rm dirs/*log
	-rm syntax/*log

install:
