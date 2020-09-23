include ../../config.mk
include ../../rules.mk

TARGETS=$(PROGRAMS)
PROGRAMS=dataswarm_manager

SOURCES=dataswarm_manager.c dataswarm_client_rep.c dataswarm_worker_rep.c dataswarm_rpc.c dataswarm_test.c
OBJECTS=$(SOURCES:%.c=%.o)
DS_OBJECTS=../comm/ds_message.o
LOCAL_CCFLAGS = -I ..
LOCAL_LINKAGE = -L ../../dttools/src -ldttools

all: $(TARGETS)

dataswarm_manager: $(OBJECTS) $(DS_OBJECTS)

clean:
	rm -rf $(OBJECTS) $(PROGRAMS)

install:
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin

test: all

.PHONY: all clean install test bindings
