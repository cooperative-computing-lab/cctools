include ../../../config.mk
include ../../../rules.mk

SOURCES = \
	ds_manager.c \
	ds_worker.c \
	ds_catalog.c \
	ds_resources.c \
	ds_json.c \
	ds_task.c \
	ds_file.c \
	ds_remote_file_info.c

PUBLIC_HEADERS = dataswarm.h ds_manager.h

OBJECTS = $(SOURCES:%.c=%.o)
PUBLIC_HEADERS = ds_manager.h ds_catalog.h ds_json.h
LIBRARIES = libdataswarm.a
TARGETS = $(LIBRARIES)

all: $(TARGETS)

libdataswarm.a: $(OBJECTS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(PUBLIC_HEADERS) $(CCTOOLS_INSTALL_DIR)/include/cctools/

clean:
	rm -rf $(OBJECTS) $(LIBRARIES)

test: all

.PHONY: all clean install test 
