CCTOOLS_HOME = ../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

TARGETS=hogmem msqrt cluster

all: ${TARGETS}

hogmem: hogmem.c 

msqrt: msqrt.c

cluster: cluster.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

test:

clean:
	-rm *.o ${TARGETS}

install:
