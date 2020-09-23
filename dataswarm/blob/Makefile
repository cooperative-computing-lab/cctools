include ../../config.mk
include ../../rules.mk

SOURCES=ds_blob.c ds_blob_table.c
OBJECTS=$(SOURCES:%.c=%.o)
LOCAL_CCFLAGS = -I ..
LOCAL_LINKAGE = -L ../../dttools/src -ldttools

all: $(OBJECTS)

clean:
	rm -rf $(OBJECTS)

install:

test: all

.PHONY: all clean install test
