UNAME := $(shell uname)

include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
LIBRARIES = librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX)
OBJECTS = resource_monitor_pb.o rmonitor_helper_comm.o resource_monitor.o resource_monitor_tools.o rmonitor_helper.o
PROGRAMS = resource_monitor resource_monitor_histograms piggybacker rmonitor_poll_example
TARGETS = $(LIBRARIES) $(PROGRAMS)

all: $(TARGETS)

librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX): rmonitor_helper.o rmonitor_helper_comm.o $(EXTERNAL_DEPENDENCIES)
	$(CCTOOLS_CC) -shared -fPIC $^ -o $@ -ldl

rmonitor_piggyback.h: librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX) piggybacker
	./piggybacker rmonitor_piggyback.h CCTOOLS_USE_RMONITOR_HELPER_LIB librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX)

resource_monitor.o resource_monitor_pb.o: rmonitor_piggyback.h
resource_monitor: rmonitor_helper_comm.o

resource_monitor_histograms.o resource_monitor_cluster.o resource_monitor_tools.o: %.o: %.c
	$(CCTOOLS_CC) -fopenmp -g -O3 -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $<

resource_monitor_histograms resource_monitor_cluster: %: resource_monitor_tools.o %.o
	$(CCTOOLS_LD) -fopenmp -g -O3 -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)

rmonitor_poll_example: rmonitor_poll_example.o
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)

$(PROGRAMS): $(EXTERNAL_DEPENDENCIES)

resource_monitor_pb.o: resource_monitor.c
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) -DCCTOOLS_USE_RMONITOR_HELPER_LIB $<
resource_monitor: resource_monitor_pb.o
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)

clean:
	rm -f $(OBJECTS) $(TARGETS) $(PROGRAMS) resource_monitor_pb.* rmonitor_piggyback.h* *.o

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/

test: all

.PHONY: all clean install test
