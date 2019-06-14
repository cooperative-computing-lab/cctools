UNAME := $(shell uname)

include ../../config.mk
include ../../rules.mk

LIBRARIES = librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX) librmonitor_helper_f.$(CCTOOLS_DYNAMIC_SUFFIX)
OBJECTS = resource_monitor_pb.o rmonitor_helper_comm.o resource_monitor.o resource_monitor_tools.o rmonitor_helper.o rmonitor_file_watch.o

LOCAL_LINKAGE = ../../dttools/src/libdttools.a

PROGRAMS = resource_monitor piggybacker rmonitor_poll_example rmonitor_snapshot

ifneq ($(CCTOOLS_STATIC),1)
PROGRAMS += resource_monitor_histograms
endif

TARGETS = $(LIBRARIES) $(PROGRAMS)

all: $(TARGETS) bindings

# Note below: we use gcc on static builds so that the library gets to use the system linker.
librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX): rmonitor_helper.o rmonitor_helper_comm.o
ifeq ($(CCTOOLS_STATIC),1)
	gcc           -shared -Wl,-init,rmonitor_helper_initialize -fPIC $^ -o $@ -ldl $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)
else
	$(CCTOOLS_CC) -shared -Wl,-init,rmonitor_helper_initialize -fPIC $^ -o $@ -ldl $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)
endif

piggybacker: piggybacker.o

rmonitor_piggyback.h: librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX) piggybacker
	./piggybacker rmonitor_piggyback.h librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX)

resource_monitor_histograms: resource_monitor_histograms.o resource_monitor_tools.o
ifeq ($(CCTOOLS_STATIC),1)
	@echo "resource_monitor_histograms cannot be built statically"
else
	$(CCTOOLS_LD) -O3 -fopenmp -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)
endif

resource_monitor.o: resource_monitor.c rmonitor_piggyback.h

resource_monitor: resource_monitor.o rmonitor_helper_comm.o rmonitor_file_watch.o

rmonitor_snapshot: rmonitor_snapshot.o rmonitor_helper_comm.o

rmonitor_poll_example: rmonitor_poll_example.o

bindings:
	$(MAKE) -C bindings

clean:
	rm -f $(OBJECTS) $(TARGETS) $(PROGRAMS) resource_monitor_pb.* rmonitor_piggyback.h* *.o
	$(MAKE) -C bindings clean

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	$(MAKE) -C bindings install

test: all

.PHONY: all clean install test bindings
