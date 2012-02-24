CCTOOLS_HOME = ../..

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

all:

test-makeflow:
	@echo running makeflow tests
	@${CCTOOLS_HOME}/dttools/src/test_runner.sh TR_[0-9]*.sh

test-starch:
	@echo running starch tests
	@${CCTOOLS_HOME}/dttools/src/test_runner.sh TR_starch*.sh

test: test-makeflow test-starch

clean:

install:
