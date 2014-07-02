include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
HEADERS_PUBLIC = ftp_lite.h
LIBRARIES = libftp_lite.a
OBJECTS = ftp_lite.o error.o radix.o login.o stream.o network.o
PROGRAMS = ftp_lite_test ftp_lite_copy
TARGETS = $(PROGRAMS) $(LIBRARIES)

all: $(TARGETS)

libftp_lite.a: $(OBJECTS)
$(PROGRAMS): libftp_lite.a $(EXTERNAL_DEPENDENCIES)

test:

clean:
	rm -f $(OBJECTS) $(TARGETS)

install:
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(HEADERS_PUBLIC) $(CCTOOLS_INSTALL_DIR)/include/cctools/
