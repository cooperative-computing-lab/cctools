include ../../../config.mk
include ../../../rules.mk

LOCAL_LINKAGE+=${CCTOOLS_HOME}/taskvine/src/manager/libtaskvine.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_CCFLAGS=-I ${CCTOOLS_HOME}/taskvine/src/manager

SOURCES = \
	vine_sandbox.c \
	vine_cache.c \
	vine_transfer.c \
	vine_transfer_server.c \
	vine_process.c \
	vine_watcher.c \
	vine_gpus.c

OBJECTS = $(SOURCES:%.c=%.o)
PROGRAMS = vine_worker
TARGETS = $(PROGRAMS)

all: $(TARGETS)

vine_worker: $(OBJECTS) $(EXTERNALS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/

clean:
	rm -rf $(PROGRAMS) *.o

test: all

lint:
	if ( ! clang-format -Werror --dry-run --style='file:../../../.clang-format' $(SOURCES) vine_worker.c);\
	then\
        echo  "========================================================";\
        echo  "NOTICE: Run `make format` to format your latest changes.";\
        echo  "========================================================";\
		exit 1;\
    fi

format:
	clang-format -i $(SOURCES) vine_worker.c

.PHONY: all clean install test lint format
