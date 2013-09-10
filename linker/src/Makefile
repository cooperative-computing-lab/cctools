CCTOOLS_HOME = ../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS= linker
DRIVERS= python_driver

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

linker: linker.o  ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

clean:
	rm -f core *~ *.o *.a ${TARGETS}

test: all

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	cp linker ${DRIVERS} ${CCTOOLS_INSTALL_DIR}/bin

