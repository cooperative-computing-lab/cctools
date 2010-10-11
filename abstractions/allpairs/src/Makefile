CCTOOLS_HOME = ../../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS = allpairs_multicore allpairs_master

LOCAL_LDFLAGS=../../../sand/src/libsandtools.a -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

allpairs_multicore: allpairs_multicore.o allpairs_compare.o
	${CCTOOLS_LD} -o $@ $^ -lpthread ${LOCAL_LDFLAGS}

allpairs_master: allpairs_master.o allpairs_compare.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

test:

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install allpairs_multicore ${CCTOOLS_INSTALL_DIR}/bin
	install allpairs_master ${CCTOOLS_INSTALL_DIR}/bin
