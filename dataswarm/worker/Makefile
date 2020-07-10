include ../../config.mk
include ../../rules.mk

TARGETS=$(PROGRAMS)
PROGRAMS=dataswarm_worker

SOURCES=dataswarm_worker.c
OBJECTS=$(SOURCES:%.c=%.o)
LOCAL_LINKAGE = -L ../../dttools/src -ldttools

all: $(TARGETS)

dataswarm_worker: $(OBJECTS)

clean:
	rm -rf $(OBJECTS) $(PROGRAMS)

install:
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin

test: all

.PHONY: all clean install test bindings
