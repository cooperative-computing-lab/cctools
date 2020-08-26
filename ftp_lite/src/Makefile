include ../../config.mk
include ../../rules.mk

LOCAL_CCFLAGS=$(CCTOOLS_GLOBUS_CCFLAGS)
LOCAL_LINKAGE=$(CCTOOLS_GLOBUS_LDFLAGS)

EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
HEADERS_PUBLIC = ftp_lite.h
LIBRARIES = libftp_lite.a
OBJECTS = ftp_lite.o error.o radix.o login.o network.o
PROGRAMS = ftp_lite_test ftp_lite_copy
TARGETS = $(PROGRAMS) $(LIBRARIES)

all: $(TARGETS)

libftp_lite.a: $(OBJECTS)
$(PROGRAMS): libftp_lite.a $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(OBJECTS) $(TARGETS) *.o

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(HEADERS_PUBLIC) $(CCTOOLS_INSTALL_DIR)/include/cctools/

test: all

.PHONY: all clean install test
