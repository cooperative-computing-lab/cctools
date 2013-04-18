CCTOOLS_HOME = ../..

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

all clean:
	cd data-structures && $(MAKE) $@ || cd ..

test:
	@echo running dttools tests
	@${CCTOOLS_HOME}/dttools/src/test_runner.sh TR_*.sh

install:

.PHONY: all install test clean

