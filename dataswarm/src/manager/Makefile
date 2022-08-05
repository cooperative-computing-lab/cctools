include ../../../config.mk
include ../../../rules.mk

SOURCES = \
	work_queue.c \
	ds_catalog.c \
	ds_resources.c \
	ds_json.c

PUBLIC_HEADERS = ds_manager.h

OBJECTS = $(SOURCES:%.c=%.o)
PUBLIC_HEADERS = ds_manager.h ds_catalog.h ds_json.h
LIBRARIES = libwork_queue.a
TARGETS = $(LIBRARIES)

all: $(TARGETS)

libwork_queue.a: $(OBJECTS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(PUBLIC_HEADERS) $(CCTOOLS_INSTALL_DIR)/include/cctools/

clean:
	rm -rf $(OBJECTS) $(LIBRARIES)

test: all

.PHONY: all clean install test 
