SOURCES = \
	  filetrace_ld.c

include ../../config.mk
include ../../rules.mk

LOCAL_LINKAGE = ../../dttools/src/libdttools.a

LIBRARIES = filetrace_ld.$(CCTOOLS_DYNAMIC_SUFFIX)
OBJECTS = filetrace_ld.o

TARGETS = $(LIBRARIES)

filetrace_ld.so: filetrace_ld.c
	$(CCTOOLS_CC) -shared -fPIC filetrace_ld.c $(CCTOOLS_INTERNAL_CCFLAGS) -ldl $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE) -o filetrace_ld.so

all: $(TARGETS)

.PHONY: all
