CCTOOLS_HOME = ../../

UNAME := $(shell uname)

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS = resource_monitor
LIBRARIES = librmonitor_helper.so

ifeq (${UNAME}, Darwin)
LIBRARIES = 
endif

LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS} ${LIBRARIES}

resource_monitor: rmonitor_helper_comm.o resource_monitor.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

librmonitor_helper.so: rmonitor_helper.o rmonitor_helper_comm.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_CC} -shared -ldl -fPIC $^ -o $@


clean:
	rm -f core *~ *.o *.a ${TARGETS} 

test: all

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	cp ${TARGETS} ${CCTOOLS_INSTALL_DIR}/bin
	mkdir -p ${CCTOOLS_INSTALL_DIR}/lib


#cp ${LIBRARIES} ${CCTOOLS_INSTALL_DIR}/lib
