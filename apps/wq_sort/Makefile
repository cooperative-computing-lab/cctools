CCTOOLS_HOME = ../../

include ${CCTOOLS_HOME}/config.mk
include ${CCTOOLS_HOME}/rules.mk

PROGRAMS = wq_sort

LOCAL_LDFLAGS= -lwork_queue -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${PROGRAMS}

wq_sort: wq_sort.o
	${CCTOOLS_LD} wq_sort.o ${LOCAL_LDFLAGS} -o $@

clean:
	rm -f *~ *.o ${PROGRAMS}

test: all

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	cp ${PROGRAMS} ${CCTOOLS_INSTALL_DIR}/bin
