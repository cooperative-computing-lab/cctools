CCTOOLS_HOME = ../../..

include ${CCTOOLS_HOME}/Makefile.config

ccflags=${CCTOOLS_PYTHON_CCFLAGS} -I${CCTOOLS_HOME}/dttools/src
ldflags=${CCTOOLS_PYTHON_LDFLAGS} ${CCTOOLS_HOME}/dttools/src/libdttools.a

all: work_queue.py _work_queue.so

work_queue.py: ../work_queue.i work_queue.binding.py
	@echo "SWIG work_queue.i (python)"
	@swig -python -I${CCTOOLS_HOME}/dttools/src -o work_queue_wrap.c -outdir . ../work_queue.i
	@cat work_queue.binding.py >> work_queue.py

work_queue_wrap.o: work_queue_wrap.c
	${CCTOOLS_CC} -fPIC -c $< -o $@ ${ccflags}

_work_queue.so: work_queue_wrap.o
	${CCTOOLS_LD} -shared $< -o $@ ${CCTOOLS_INTERNAL_LDFLAGS} ${ldflags}

test: all
	@echo running work queue python binding tests
	@${CCTOOLS_HOME}/dttools/src/test_runner.sh TR_*.sh

clean:
	rm -fr _work_queue.so work_queue.py work_queue_wrap.* *.pyc

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/lib/python${CCTOOLS_PYTHON_VERSION}/site-packages
	install _work_queue.so ${CCTOOLS_INSTALL_DIR}/lib/python${CCTOOLS_PYTHON_VERSION}/site-packages
	install  work_queue.py ${CCTOOLS_INSTALL_DIR}/lib/python${CCTOOLS_PYTHON_VERSION}/site-packages
