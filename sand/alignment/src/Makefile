CCTOOLS_HOME = ../../..
SAND_HOME = ../../
include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS =  sand_align_master sand_banded_alignment sand_sw_alignment

LOCAL_LDFLAGS= -ldttools -lsandtools ${CCTOOLS_INTERNAL_LDFLAGS} 

all: ${TARGETS}

sand_align_master: sand_align_master.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

sand_banded_alignment: sand_banded_alignment.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

sand_sw_alignment: sand_sw_alignment.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 


test:

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install sand_align_master ${CCTOOLS_INSTALL_DIR}/bin/
	install sand_banded_alignment ${CCTOOLS_INSTALL_DIR}/bin/
	install sand_sw_alignment ${CCTOOLS_INSTALL_DIR}/bin/

