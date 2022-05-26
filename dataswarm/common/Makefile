include ../../config.mk
include ../../rules.mk

SOURCES=ds_mount.c ds_resources.c ds_task.c ds_message.c ds_blob.c ds_task_attempt.c
OBJECTS=$(SOURCES:%.c=%.o)
LOCAL_LINKAGE = -L ../../dttools/src -ldttools

all: libdataswarm_common.a

libdataswarm_common.a: $(OBJECTS)
	ar rv $@ $^

clean:
	rm -rf $(OBJECTS) libdataswarm_common.a

install:

test: all

.PHONY: all clean install test
