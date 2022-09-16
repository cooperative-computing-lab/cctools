include ../../../config.mk
include ../../../rules.mk

SOURCES = \
	dataswarm_json.c \
	ds_manager.c \
	ds_manager_get.c \
	ds_manager_put.c \
	ds_schedule.c \
	ds_worker_info.c \
	ds_catalog.c \
	ds_resources.c \
	ds_task.c \
	ds_file.c \
	ds_txn_log.c \
	ds_perf_log.c \
	ds_remote_file_info.c \
	ds_factory_info.c \
	ds_task_info.c \
	ds_blocklist.c

PUBLIC_HEADERS = dataswarm.h dataswarm_json.h

OBJECTS = $(SOURCES:%.c=%.o)
PUBLIC_HEADERS = dataswarm.h dataswarm_json.h
LIBRARIES = libdataswarm.a
TARGETS = $(LIBRARIES)

all: $(TARGETS)

libdataswarm.a: $(OBJECTS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(PUBLIC_HEADERS) $(CCTOOLS_INSTALL_DIR)/include/cctools/

clean:
	rm -rf $(OBJECTS) $(LIBRARIES)

test: all

.PHONY: all clean install test 
