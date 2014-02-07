CCTOOLS_HOME = ../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

PROGRAMS =  wavefront wavefront_master

LOCAL_LDFLAGS=-lwork_queue -lchirp -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${PROGRAMS}


wavefront: wavefront.o ${CCTOOLS_HOME}/dttools/src/libdttools.a ${CCTOOLS_HOME}/chirp/src/libchirp.a
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

wavefront_master: wavefront_master.o ${CCTOOLS_HOME}/dttools/src/libdttools.a ${CCTOOLS_HOME}/chirp/src/libchirp.a
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

test:

clean:
	rm -f core *~ *.o *.a ${PROGRAMS} 

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	cp ${PROGRAMS} ${CCTOOLS_INSTALL_DIR}/bin

