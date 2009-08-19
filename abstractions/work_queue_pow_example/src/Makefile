CCTOOLS_HOME = ../../..

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS =  work_queue_pow_example

LOCAL_LDFLAGS= -lchirp -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

work_queue_pow_example: work_queue_pow_example.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 


test:

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install work_queue_pow_example ${CCTOOLS_INSTALL_DIR}/bin/


