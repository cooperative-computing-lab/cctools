CCTOOLS_HOME = ../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

SCRIPTS = wq_maker
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
	chmod 755 ${SCRIPTS}	
	cp ${SCRIPTS} ${PROGRAMS} ${CCTOOLS_INSTALL_DIR}/bin
