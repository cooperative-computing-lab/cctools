CCTOOLS_HOME = ../../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS =  wavefront wavefront_master

LOCAL_LDFLAGS= -lchirp -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}


wavefront: wavefront.o 
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

wavefront_master: wavefront_master.o 
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

test:

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install wavefront_master ${CCTOOLS_INSTALL_DIR}/bin

