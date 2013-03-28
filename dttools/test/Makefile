CCTOOLS_HOME = ../..

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

test: extra structures

all clean:
	cd data-structures && $(MAKE) $@ || cd ..

extra:
	@echo running dttools tests
	@${CCTOOLS_HOME}/dttools/src/test_runner.sh TR_*.sh

structures:
	cd data-structures && $(MAKE) test || cd ..

install:

.PHONY: all install test clean

