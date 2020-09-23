include ../../config.mk
include ../../rules.mk

SOURCES=ds_mount.c ds_process.c ds_resources.c ds_task.c
OBJECTS=$(SOURCES:%.c=%.o)
LOCAL_LINKAGE = -L ../../dttools/src -ldttools

all: $(OBJECTS)

clean:
	rm -rf $(OBJECTS)

install:

test: all

.PHONY: all clean install test
