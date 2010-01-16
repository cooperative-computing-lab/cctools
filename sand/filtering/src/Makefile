CCTOOLS_HOME = ../../..

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS =  filter_master filter_mer_seq

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

filter_master: filter_master.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

filter_mer_seq: filter_mer_seq.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

test:

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install filter_master ${CCTOOLS_INSTALL_DIR}/bin/
	install filter_mer_seq ${CCTOOLS_INSTALL_DIR}/bin/

