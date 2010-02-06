CCTOOLS_HOME = ../../..
SAND_HOME = ../../
include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS =  sand_align banded_alignment sw_alignment

LOCAL_LDFLAGS= -ldttools -lsandtools ${CCTOOLS_INTERNAL_LDFLAGS} 

all: ${TARGETS}

sand_align: sand_align.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

banded_alignment: banded_alignment.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

sw_alignment: sw_alignment.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 


test:

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install sand_align ${CCTOOLS_INSTALL_DIR}/bin/
	install banded_alignment ${CCTOOLS_INSTALL_DIR}/bin/
	install sw_alignment ${CCTOOLS_INSTALL_DIR}/bin/

