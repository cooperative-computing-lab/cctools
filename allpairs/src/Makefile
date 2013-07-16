CCTOOLS_HOME = ../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS = allpairs_multicore allpairs_master

LOCAL_LDFLAGS=../../sand/src/libsandtools.a -lwork_queue -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

allpairs_multicore: allpairs_multicore.o allpairs_compare.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_LD} -o $@ allpairs_multicore.o allpairs_compare.o -lpthread ${LOCAL_LDFLAGS}

allpairs_master: allpairs_master.o allpairs_compare.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_LD} -o $@ allpairs_master.o allpairs_compare.o ${LOCAL_LDFLAGS}

test:

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	cp ${TARGETS} ${CCTOOLS_INSTALL_DIR}/bin
