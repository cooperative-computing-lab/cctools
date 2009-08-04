CCTOOLS_HOME = ../../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: map

map: map.o
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

test:

clean:
	rm -f core *~ *.o *.a map

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install map ${CCTOOLS_INSTALL_DIR}/bin
