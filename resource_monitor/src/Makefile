UNAME := $(shell uname)

include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
LIBRARIES = librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX)
OBJECTS = resource_monitor_pb.o rmonitor_helper_comm.o resource_monitor.o rmon_tools.o rmonitor_helper.o
PROGRAMS = resource_monitorv resource_monitor resource_monitor_cluster resource_monitor_histograms piggybacker rmonitor_poll_example
SCRIPTS = resource_monitor_visualizer.py
TARGETS = $(LIBRARIES) $(PROGRAMS)

ifeq ($(UNAME), Darwin)
PROGRAMS = resource_monitorv piggybacker
endif

all: $(TARGETS)

ifeq ($(UNAME), FreeBSD)
librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX): rmonitor_helper.o rmonitor_helper_comm.o $(EXTERNAL_DEPENDENCIES)
	$(CCTOOLS_CC) -shared -Wl,-soname,$@ -lc -fPIC $^ -o $@
else
librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX): rmonitor_helper.o rmonitor_helper_comm.o $(EXTERNAL_DEPENDENCIES)
	$(CCTOOLS_CC) -shared -ldl -fPIC $^ -o $@
endif

ifeq ($(UNAME), Darwin)
rmonitor_piggyback.h: piggybacker
	./piggybacker rmonitor_piggyback.h CCTOOLS_USE_RMONITOR_HELPER_LIB
else
rmonitor_piggyback.h: librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX) piggybacker
	./piggybacker rmonitor_piggyback.h CCTOOLS_USE_RMONITOR_HELPER_LIB librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX)
endif

resource_monitor.o resource_monitor_pb.o: rmonitor_piggyback.h
resource_monitor resource_monitorv: rmonitor_helper_comm.o
resource_monitor_cluster resource_monitor_histograms: rmon_tools.o

rmonitor_poll_example: rmonitor_poll_example.o
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)

$(PROGRAMS): $(EXTERNAL_DEPENDENCIES)

resource_monitor_pb.o: resource_monitor.c
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) -DCCTOOLS_USE_RMONITOR_HELPER_LIB $<
resource_monitor: resource_monitor_pb.o
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)
resource_monitorv: resource_monitor.o
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)

clean:
	rm -f $(OBJECTS) $(TARGETS) resource_monitor_pb.* rmonitor_piggyback.h*

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/
	cp $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	cp -r resource_monitor_visualizer_static $(CCTOOLS_INSTALL_DIR)/lib

test: all

.PHONY: all clean install test
