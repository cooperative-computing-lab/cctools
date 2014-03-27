CCTOOLS_HOME = ../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

TARGETS=cluster

all: ${TARGETS}

cluster: cluster.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

clean:
	-rm *.o ${TARGETS}

install:
