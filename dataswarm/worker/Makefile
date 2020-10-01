include ../../config.mk
include ../../rules.mk

TARGETS=$(PROGRAMS)
PROGRAMS=ds_worker

SOURCES=ds_worker_main.c ds_worker.c ds_process.c ds_task_table.c ds_blob_table.c ds_measure.c
OBJECTS=$(SOURCES:%.c=%.o)

LOCAL_CCFLAGS = -I ../common
LOCAL_LINKAGE = -L ../common -ldataswarm_common -L ../../dttools/src -ldttools

all: $(TARGETS)

ds_worker: $(OBJECTS)

clean:
	rm -rf $(OBJECTS) $(PROGRAMS)

install:
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin

test: all

.PHONY: all clean install test bindings
