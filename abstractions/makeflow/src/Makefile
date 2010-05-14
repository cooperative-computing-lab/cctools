CCTOOLS_HOME = ../../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS = makeflow

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

makeflow: makeflow.o
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

test: all
	cd test/ && make test && cd ..

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install makeflow ${CCTOOLS_INSTALL_DIR}/bin
	chmod 755 condor_submit_makeflow
	install condor_submit_makeflow ${CCTOOLS_INSTALL_DIR}/bin
	install makeflow_log_parser ${CCTOOLS_INSTALL_DIR}/bin
	install monitor_makeflow ${CCTOOLS_INSTALL_DIR}/bin
