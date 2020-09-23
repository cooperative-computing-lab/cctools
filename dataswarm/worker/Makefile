include ../../config.mk
include ../../rules.mk

TARGETS=$(PROGRAMS)
PROGRAMS=dataswarm_worker

SOURCES=dataswarm_worker_main.c dataswarm_worker.c dataswarm_task_table.c
OBJECTS=$(SOURCES:%.c=%.o)
DS_OBJECTS=../comm/ds_message.o ../task/ds_process.o ../task/ds_resources.o ../task/ds_task.o ../task/ds_mount.o ../blob/ds_blob.o ../blob/ds_blob_table.o

LOCAL_CCFLAGS = -I ..
LOCAL_LINKAGE = -L ../../dttools/src -ldttools

all: $(TARGETS)

dataswarm_worker: $(OBJECTS) $(DS_OBJECTS)

clean:
	rm -rf $(OBJECTS) $(PROGRAMS)

install:
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin

test: all

.PHONY: all clean install test bindings
