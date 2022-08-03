include ../../../config.mk
include ../../../rules.mk

SOURCES = \
	work_queue_sandbox.c \
	work_queue_cache.c \
	work_queue_process.c \
	work_queue_coprocess.c \
	work_queue_watcher.c \
	work_queue_gpus.c

OBJECTS = $(SOURCES:%.c=%.o)

LIBDTTOOLS = ${CCTOOLS_HOME}/dttools/src/libdttools.a
EXTERNALS = $(LIBDTTOOLS) ../manager/libwork_queue.a
EXTERNAL_DEPENDENCIES = ../../../dttools/src/libdttools.a 
PROGRAMS = work_queue_worker
TARGETS = $(PROGRAMS)

all: $(TARGETS)

work_queue_worker: $(OBJECTS) $(EXTERNALS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/

clean:
	rm -rf $(OBJECTS) $(PROGRAMS) 

test: all

.PHONY: all clean install test
