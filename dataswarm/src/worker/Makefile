include ../../../config.mk
include ../../../rules.mk

SOURCES = \
	ds_sandbox.c \
	ds_cache.c \
	ds_process.c \
	ds_coprocess.c \
	ds_watcher.c \
	ds_gpus.c

OBJECTS = $(SOURCES:%.c=%.o)

LIBDTTOOLS = ${CCTOOLS_HOME}/dttools/src/libdttools.a
EXTERNALS = $(LIBDTTOOLS) ../manager/libwork_queue.a
EXTERNAL_DEPENDENCIES = ../../../dttools/src/libdttools.a 
PROGRAMS = ds_worker
TARGETS = $(PROGRAMS)

all: $(TARGETS)

ds_worker: $(OBJECTS) $(EXTERNALS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/

clean:
	rm -rf $(OBJECTS) $(PROGRAMS) 

test: all

.PHONY: all clean install test
