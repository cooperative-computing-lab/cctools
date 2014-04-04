CCTOOLS_HOME = ../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

TARGETS=hogmem msqrt

all: ${TARGETS}

hogmem: hogmem.c 

msqrt: msqrt.c

test:

clean:
	-rm *.o ${TARGETS}

install:
