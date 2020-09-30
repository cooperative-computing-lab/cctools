include ../../config.mk
include ../../rules.mk

TARGETS=$(PROGRAMS)
PROGRAMS=ds_worker

SOURCES=ds_worker_main.c ds_worker.c ds_process.c ds_task_table.c ds_blob_table.c ds_hash.c
OBJECTS=$(SOURCES:%.c=%.o)
DS_OBJECTS=../common/ds_message.o ../common/ds_resources.o ../common/ds_task.o ../common/ds_mount.o ../common/ds_blob.o

LOCAL_CCFLAGS = -I ..
LOCAL_LINKAGE = -L ../../dttools/src -ldttools

all: $(TARGETS)

ds_worker: $(OBJECTS) $(DS_OBJECTS)

clean:
	rm -rf $(OBJECTS) $(PROGRAMS)

install:
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin

test: all

.PHONY: all clean install test bindings
