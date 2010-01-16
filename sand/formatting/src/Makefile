CCTOOLS_HOME = ../../..

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS =  ucopy uncompress_reads compress_reads

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

ucopy: ucopy.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

uncompress_reads: uncompress_reads.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

compress_reads: compress_reads.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

test:

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install ucopy ${CCTOOLS_INSTALL_DIR}/bin/
	install uncompress_reads ${CCTOOLS_INSTALL_DIR}/bin/
	install compress_reads ${CCTOOLS_INSTALL_DIR}/bin/

