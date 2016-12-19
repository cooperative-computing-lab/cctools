UNAME := $(shell uname)

include ../../config.mk
include ../../rules.mk

STATIC_BUILD=$(STATIC)
CCTOOLS_DEPENDENCIES = ../../dttools/src/libdttools.a
LIBRARIES = librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX)
OBJECTS = resource_monitor_pb.o rmonitor_helper_comm.o resource_monitor.o resource_monitor_tools.o rmonitor_helper.o
PROGRAMS = resource_monitor resource_monitor_histograms piggybacker rmonitor_poll_example

# currently only python bindings have been implemented
CCTOOLS_SWIG_BINDINGS := $(findstring python, $(CCTOOLS_SWIG_BINDINGS))

TARGETS = $(LIBRARIES) $(PROGRAMS)

ifeq ($(STATIC_BUILD),yes)
	STATIC_FLAG=-static
else
	STATIC_FLAG=
	CCTOOLS_DEPENDENCIES+=$(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)
endif

all: $(TARGETS) bindings

librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX): rmonitor_helper.o rmonitor_helper_comm.o
	$(CCTOOLS_LD) -shared -fPIC $^ -o $@ -ldl $(CCTOOLS_DEPENDENCIES)

piggybacker: piggybacker.c
	$(CCTOOLS_CC) $(STATIC_FLAG) $(CCTOOLS_INTERNAL_CCFLAGS) $^ $(CCTOOLS_DEPENDENCIES) -o $@

rmonitor_piggyback.h: librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX) piggybacker
	./piggybacker rmonitor_piggyback.h CCTOOLS_USE_RMONITOR_HELPER_LIB librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX)

resource_monitor_histograms: resource_monitor_histograms.c resource_monitor_tools.o
	$(CCTOOLS_CC) $(STATIC_FLAG) $^ -fopenmp -g -O3 -o $@ $(CCTOOLS_INTERNAL_CCFLAGS) $(CCTOOLS_DEPENDENCIES)

$(PROGRAMS): ../../dttools/src/libdttools.a

resource_monitor_pb.o: resource_monitor.c rmonitor_piggyback.h
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) -DCCTOOLS_USE_RMONITOR_HELPER_LIB $<

resource_monitor: resource_monitor_pb.o rmonitor_helper_comm.o
	$(CCTOOLS_CC) $(STATIC_FLAG) -o $@ $(CCTOOLS_INTERNAL_CCFLAGS) $^ $(CCTOOLS_DEPENDENCIES)

rmonitor_poll_example: rmonitor_poll_example.o
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_CCFLAGS) $^ $(CCTOOLS_DEPENDENCIES)


.PHONY: $(CCTOOLS_SWIG_BINDINGS)
bindings: $(CCTOOLS_SWIG_BINDINGS)
$(CCTOOLS_SWIG_BINDINGS):
	@$(MAKE) -C $@

CCTOOLS_SWIG_BINDINGS_INSTALL = $(CCTOOLS_SWIG_BINDINGS:%=install-%)
install-bindings: $(CCTOOLS_SWIG_BINDINGS_INSTALL)
$(CCTOOLS_SWIG_BINDINGS_INSTALL): $(CCTOOLS_SWIG_BINDINGS)
	@$(MAKE) -C $(@:install-%=%) install

CCTOOLS_SWIG_BINDINGS_CLEAN = $(CCTOOLS_SWIG_BINDINGS:%=clean-%)
clean-bindings: $(CCTOOLS_SWIG_BINDINGS_CLEAN)
$(CCTOOLS_SWIG_BINDINGS_CLEAN):
	@$(MAKE) -C $(@:clean-%=%) clean

clean: clean-bindings
	rm -f $(OBJECTS) $(TARGETS) $(PROGRAMS) resource_monitor_pb.* rmonitor_piggyback.h* *.o

install: all install-bindings
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/

test: all

.PHONY: all clean install test
