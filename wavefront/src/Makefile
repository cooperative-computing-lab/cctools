CCTOOLS_HOME = ../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS =  wavefront wavefront_master

LOCAL_LDFLAGS=-ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}


wavefront: wavefront.o 
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

wavefront_master: wavefront_master.o 
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

test:

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

install: all
	${CCTOOLS_INSTALL} -d ${CCTOOLS_INSTALL_DIR}/
	${CCTOOLS_INSTALL} -d ${CCTOOLS_INSTALL_DIR}/bin
	${CCTOOLS_INSTALL} wavefront ${CCTOOLS_INSTALL_DIR}/bin
	${CCTOOLS_INSTALL} wavefront_master ${CCTOOLS_INSTALL_DIR}/bin

