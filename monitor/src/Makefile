CCTOOLS_HOME = ../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS = monitor

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

monitor: monitor.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_LD} monitor.o ${LOCAL_LDFLAGS} -o $@

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

test: all

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	cp monitor ${CCTOOLS_INSTALL_DIR}/bin
