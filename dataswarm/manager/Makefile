include ../../config.mk
include ../../rules.mk

TARGETS=$(PROGRAMS)
PROGRAMS=ds_manager

SOURCES=ds_manager.c ds_client_rep.c ds_worker_rep.c ds_rpc.c ds_test.c ds_client_ops.c ds_validate.c
OBJECTS=$(SOURCES:%.c=%.o)
DS_OBJECTS=../common/ds_message.o
LOCAL_CCFLAGS = -I ..
LOCAL_LINKAGE = -L ../../dttools/src -ldttools

all: $(TARGETS)

ds_manager: $(OBJECTS) $(DS_OBJECTS)

clean:
	rm -rf $(OBJECTS) $(PROGRAMS)

install:
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin

test: all

.PHONY: all clean install test bindings
