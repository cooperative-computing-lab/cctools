CCTOOLS_HOME = ../..

include ../../Makefile.config

all: setup.cfg
	@${CCTOOLS_PYTHON} ./setup.py build_ext --inplace

test: workqueue.so 
	@echo running python-workqueue tests
	@env CCTOOLS_PYTHON=${CCTOOLS_PYTHON} ${CCTOOLS_HOME}/dttools/src/test_runner.sh TR_*.sh

clean:
	rm -fr build workqueue.so

install: all
	@${CCTOOLS_PYTHON} ./setup.py install --prefix ${CCTOOLS_INSTALL_DIR}

setup.cfg:
	@${CCTOOLS_PYTHON} ./configure.py -i ../../dttools/src -l ../../dttools/src
