CCTOOLS_HOME = ../../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

TARGETS=set

all: ${TARGETS}

set: set.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

test:
	@echo running data structure tests
	@${CCTOOLS_HOME}/dttools/src/test_runner.sh TR_*_data_struct_*.sh

clean:
	-rm *.o ${TARGETS}

install:
