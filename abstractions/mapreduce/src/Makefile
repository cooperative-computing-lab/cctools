CCTOOLS_HOME = ../../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS = mapreduce mr_map mr_reduce mr_merge
SCRIPTS = mr_wrapper.sh

LOCAL_LDFLAGS=-ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: mr_config.h ${TARGETS}

mr_config.h:
	echo "#define CCTOOLS_INSTALL_DIR \"${CCTOOLS_INSTALL_DIR}\"" > mr_config.h

mr_map: mr_map.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm

mr_reduce: mr_reduce.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm

mr_merge: mr_merge.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm

mapreduce: mapreduce.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm

test:

clean:
	rm -f core *~ *.o *.a mr_config.h ${TARGETS} 

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	for f in ${TARGETS}; do install -m 755 $$f ${CCTOOLS_INSTALL_DIR}/bin/$$f; done
	for f in ${SCRIPTS}; do install -m 755 $$f ${CCTOOLS_INSTALL_DIR}/bin/$$f; done
