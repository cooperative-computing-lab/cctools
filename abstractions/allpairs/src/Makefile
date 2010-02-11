CCTOOLS_HOME = ../../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS = allpairs allpairs_multicore allpairs_status allpairs_wait allpairs_cleanup allpairs_workqueue

LOCAL_LDFLAGS=-lchirp -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

allpairs: allpairs.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm #-lefence

allpairs_status: allpairs_status.o ${CCTOOLS_HOME}/chirp/src/libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

allpairs_wait: allpairs_wait.o ${CCTOOLS_HOME}/chirp/src/libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

allpairs_cleanup: allpairs_cleanup.o ${CCTOOLS_HOME}/chirp/src/libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

allpairs_multicore: allpairs_multicore.o allpairs_util.o ${CCTOOLS_HOME}/chirp/src/libchirp.a
	${CCTOOLS_LD} -o $@ $^ -lpthread ${LOCAL_LDFLAGS}

allpairs_workqueue: allpairs_workqueue.o allpairs_util.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

test:

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install allpairs ${CCTOOLS_INSTALL_DIR}/bin
	install allpairs_wait ${CCTOOLS_INSTALL_DIR}/bin
	install allpairs_status ${CCTOOLS_INSTALL_DIR}/bin
	install allpairs_cleanup ${CCTOOLS_INSTALL_DIR}/bin	
	install allpairs_wrapper.sh ${CCTOOLS_INSTALL_DIR}/bin
	install allpairs_multicore ${CCTOOLS_INSTALL_DIR}/bin
	install allpairs_workqueue ${CCTOOLS_INSTALL_DIR}/bin
