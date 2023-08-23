include ../../../config.mk
include ../../../rules.mk

SOURCES = \
	vine_manager.c \
	vine_manager_get.c \
	vine_manager_put.c \
	vine_manager_summarize.c \
	vine_schedule.c \
	vine_worker_info.c \
	vine_catalog.c \
	vine_resources.c \
	vine_task.c \
	vine_file.c \
	vine_mount.c \
	vine_txn_log.c \
	vine_taskgraph_log.c \
	vine_cached_name.c \
	vine_checksum.c \
	vine_perf_log.c \
	vine_file_replica.c \
	vine_factory_info.c \
	vine_task_info.c \
	vine_blocklist.c \
	vine_current_transfers.c \
	vine_file_replica_table.c \
	vine_fair.c \
	vine_runtime_dir.c

PUBLIC_HEADERS = taskvine.h

OBJECTS = $(SOURCES:%.c=%.o)
PUBLIC_HEADERS = taskvine.h
LIBRARIES = libtaskvine.a
TARGETS = $(LIBRARIES)

all: $(TARGETS)

libtaskvine.a: $(OBJECTS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(PUBLIC_HEADERS) $(CCTOOLS_INSTALL_DIR)/include/cctools/

clean:
	rm -rf $(OBJECTS) $(LIBRARIES)

test: all

lint:
	if ( ! clang-format -Werror --dry-run --style='file:../../../.clang-format' $(SOURCES));\
	then\
		echo  "========================================================";\
		echo  "NOTICE: Run `make format` to format your latest changes.";\
		echo  "========================================================";\
		exit 1;\
	fi

format:
	clang-format -i $(SOURCES)

.PHONY: all clean install test lint format
