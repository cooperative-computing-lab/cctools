CCTOOLS_HOME = ../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS = makeflow

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

makeflow: makeflow.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_LD} makeflow.o ${LOCAL_LDFLAGS} -o $@

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

test: all

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install makeflow ${CCTOOLS_INSTALL_DIR}/bin
	install -m755 condor_submit_makeflow ${CCTOOLS_INSTALL_DIR}/bin
	install makeflow_log_parser ${CCTOOLS_INSTALL_DIR}/bin
	install makeflow_monitor ${CCTOOLS_INSTALL_DIR}/bin
	sed 's|/usr/bin/env python|${CCTOOLS_PYTHON}|' starch > starch.bin
	install -m755 starch.bin ${CCTOOLS_INSTALL_DIR}/bin/starch
	rm -f starch.bin
