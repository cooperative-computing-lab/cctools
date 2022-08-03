include ../../../config.mk
include ../../../rules.mk

SOURCES = \
	work_queue.c \
	work_queue_catalog.c \
	work_queue_resources.c \
	work_queue_json.c

PUBLIC_HEADERS = work_queue.h

OBJECTS = $(SOURCES:%.c=%.o)
PUBLIC_HEADERS = work_queue.h work_queue_catalog.h work_queue_json.h
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
