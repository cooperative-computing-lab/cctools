include ../../config.mk
include ../../rules.mk

TARGETS=$(PROGRAMS)
PROGRAMS=ds_manager

SOURCES=ds_manager.c ds_scheduler.c ds_client_rep.c ds_worker_rep.c ds_rpc.c ds_test.c ds_client_ops.c ds_validate.c ds_file.c ds_catalog_update.c
OBJECTS=$(SOURCES:%.c=%.o)
LOCAL_CCFLAGS = -I ../common
LOCAL_LINKAGE = -L ../common -ldataswarm_common -L ../../dttools/src -ldttools

all: $(TARGETS)

ds_manager: $(OBJECTS)

clean:
	rm -rf $(OBJECTS) $(PROGRAMS)

install:
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin

test: all

.PHONY: all clean install test bindings
