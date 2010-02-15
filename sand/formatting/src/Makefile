CCTOOLS_HOME = ../../..
SAND_HOME = ../../
include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS =  sand_ucopy sand_uncompress_reads sand_compress_reads

LOCAL_LDFLAGS= -ldttools -lsandtools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

sand_ucopy: sand_ucopy.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

sand_uncompress_reads: sand_uncompress_reads.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

sand_compress_reads: sand_compress_reads.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

test:

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install sand_ucopy ${CCTOOLS_INSTALL_DIR}/bin/
	install sand_uncompress_reads ${CCTOOLS_INSTALL_DIR}/bin/
	install sand_compress_reads ${CCTOOLS_INSTALL_DIR}/bin/

