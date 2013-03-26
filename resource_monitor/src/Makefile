CCTOOLS_HOME = ../../

UNAME := $(shell uname)

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS = resource_monitorv resource_monitor
LIBRARIES = librmonitor_helper.so

ifeq (${UNAME}, Darwin)
LIBRARIES =
TARGETS = resource_monitorv
endif

ifeq (${UNAME}, FreeBSD)
LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS} -lkvm
else
LOCAL_LDFLAGS= -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}
endif

all: ${TARGETS} ${LIBRARIES}

resource_monitorv: rmonitor_helper_comm.o resource_monitor.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

resource_monitor: rmonitor_helper_comm.o resource_monitor_pb.o rmonitor_piggyback.h ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

resource_monitor_pb.c: resource_monitor.c
	cp resource_monitor.c resource_monitor_pb.c
	
resource_monitor_pb.o: rmonitor_piggyback.h resource_monitor_pb.c 
	${CCTOOLS_CC} -g -DCCTOOLS_USE_RMONITOR_HELPER_LIB ${CCTOOLS_INTERNAL_CCFLAGS} $^ -c

resource_monitor.o: rmonitor_piggyback.h resource_monitor.c 
	${CCTOOLS_CC} -g ${CCTOOLS_INTERNAL_CCFLAGS} $^ -c

ifeq (${UNAME}, FreeBSD)
librmonitor_helper.so: rmonitor_helper.o rmonitor_helper_comm.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_CC} -shared -Wl,-soname,$@ -lc -fPIC $^ -o $@
else
librmonitor_helper.so: rmonitor_helper.o rmonitor_helper_comm.o ${CCTOOLS_HOME}/dttools/src/libdttools.a
	${CCTOOLS_CC} -shared -ldl -fPIC $^ -o $@
endif

ifeq (${UNAME}, Darwin)
rmonitor_piggyback.h: piggybacker
	./piggybacker rmonitor_piggyback.h CCTOOLS_USE_RMONITOR_HELPER_LIB
else
rmonitor_piggyback.h: librmonitor_helper.so piggybacker
	./piggybacker rmonitor_piggyback.h CCTOOLS_USE_RMONITOR_HELPER_LIB librmonitor_helper.so
endif

piggybacker: piggybacker.o
	${CCTOOLS_LD} $^ ${LOCAL_LDFLAGS} -o $@

clean:
	rm -f core *~ *.o *.a *.so resource_monitor_pb.* rmonitor_piggyback.h* piggybacker resource_monitor resource_monitorv

test: all

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	cp ${TARGETS} ${CCTOOLS_INSTALL_DIR}/bin
	mkdir -p ${CCTOOLS_INSTALL_DIR}/lib
	-cp ${LIBRARIES} ${CCTOOLS_INSTALL_DIR}/lib
