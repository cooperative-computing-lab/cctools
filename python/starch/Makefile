CCTOOLS_HOME = ../..

include ../../Makefile.config

all:

test:
	@echo running starch tests
	@env CCTOOLS_PYTHON=${CCTOOLS_PYTHON} ${CCTOOLS_HOME}/dttools/src/test_runner.sh test/TR_*.sh

clean:
	rm -fr build

install: all
	@${CCTOOLS_PYTHON} ./setup.py install --prefix ${CCTOOLS_INSTALL_DIR}

.PHONY:	all clean install test
