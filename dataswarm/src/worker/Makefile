include ../../../config.mk
include ../../../rules.mk

LOCAL_LINKAGE+=${CCTOOLS_HOME}/dataswarm/src/manager/libdataswarm.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_CCFLAGS=-I ${CCTOOLS_HOME}/dataswarm/src/manager

SOURCES = \
	ds_sandbox.c \
	ds_cache.c \
	ds_process.c \
	ds_coprocess.c \
	ds_watcher.c \
	ds_gpus.c

OBJECTS = $(SOURCES:%.c=%.o)
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
