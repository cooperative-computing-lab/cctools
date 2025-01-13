UNAME := $(shell uname)

SOURCES = \
    rminimonitor_helper.c \
    rmonitor_helper_comm.c \
    rmonitor_helper.c \
    rmonitor_snapshot.c \
    rmonitor_file_watch.c \
    rmonitor_poll_example.c \
    piggybacker.c \
    resource_monitor.c

include ../../config.mk
include ../../rules.mk

LIBRARIES = librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX) librminimonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX)
OBJECTS = resource_monitor_pb.o rmonitor_helper_comm.o resource_monitor.o rmonitor_helper.o rmonitor_file_watch.o

LOCAL_LINKAGE = ../../dttools/src/libdttools.a

PROGRAMS = resource_monitor piggybacker rmonitor_poll_example rmonitor_snapshot

ifeq ($(CCTOOLS_LINUX_NATIVE_X86_64),yes)
	TARGETS = $(LIBRARIES) $(PROGRAMS)
else
	TARGETS =
	PROGRAMS =
endif

all: $(TARGETS) bindings

librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX): rmonitor_helper.o rmonitor_helper_comm.o
	$(CCTOOLS_CC) -shared -Wl,-init,rmonitor_helper_initialize -fPIC $^ -o $@ -ldl $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)

librminimonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX): rminimonitor_helper.o
	$(CCTOOLS_CC) -shared -Wl,-init,rmonitor_helper_initialize -fPIC $^ -o $@ -ldl $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)

piggybacker: piggybacker.o

rmonitor_piggyback.h: librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX) piggybacker
	./piggybacker rmonitor_piggyback.h librmonitor_helper.$(CCTOOLS_DYNAMIC_SUFFIX)

resource_monitor.o: resource_monitor.c rmonitor_piggyback.h

resource_monitor: resource_monitor.o rmonitor_helper_comm.o rmonitor_file_watch.o

rmonitor_snapshot: rmonitor_snapshot.o rmonitor_helper_comm.o

rmonitor_poll_example: rmonitor_poll_example.o

bindings:
	$(MAKE) -C bindings

lint:
	if ( ! clang-format -Werror --dry-run --style='file:../../.clang-format' $(SOURCES));\
		then\
		echo "========================================================";\
		echo "NOTICE: Run 'make format' to format your latest changes.";\
		echo "========================================================";\
		exit 1;\
	fi
	$(MAKE) -C bindings lint

format:
	clang-format -i $(SOURCES)

clean:
	rm -f $(OBJECTS) $(TARGETS) $(PROGRAMS) resource_monitor_pb.* rmonitor_piggyback.h* *.o
	$(MAKE) -C bindings clean

install: all
ifeq ($(CCTOOLS_LINUX_NATIVE_X86_64),yes)
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	$(MAKE) -C bindings install
else
	$(MAKE) -C bindings install
endif

test: all

.PHONY: all clean install test lint format bindings
