CCTOOLS_HOME = ../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS = resource_monitor

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

resource_monitor: resource_monitor.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_LD} resource_monitor.o ${LOCAL_LDFLAGS} -o $@

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

test: all

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	cp resource_monitor ${CCTOOLS_INSTALL_DIR}/bin
