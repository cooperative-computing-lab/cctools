CCTOOLS_HOME = ../../

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS = makeflow
SCRIPTS = condor_submit_makeflow makeflow_log_parser makeflow_monitor starch

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

makeflow: makeflow.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_LD} makeflow.o ${LOCAL_LDFLAGS} -o $@

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

test: all

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	chmod 755 ${SCRIPTS}
	cp makeflow ${SCRIPTS} ${CCTOOLS_INSTALL_DIR}/bin
