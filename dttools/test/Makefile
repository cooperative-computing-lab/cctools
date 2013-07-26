CCTOOLS_HOME = ../..

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

all clean install:

test:
	@echo running dttools tests
	@${CCTOOLS_HOME}/dttools/src/test_runner.sh TR_*.sh
	cd ${CCTOOLS_HOME}/data-structures && $(MAKE) $@


.PHONY: all install test clean

