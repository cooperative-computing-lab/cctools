include ../../../config.mk
include ../../../rules.mk

LOCAL_LINKAGE+=${CCTOOLS_HOME}/taskvine/src/manager/libtaskvine.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_CCFLAGS+=-I ${CCTOOLS_HOME}/taskvine/src/manager

PROGRAMS = ds_status ds_api_proxy ds_benchmark
SCRIPTS = ds_graph_log ds_graph_workers
TEST_PROGRAMS = ds_test
TARGETS = $(PROGRAMS) $(TEST_PROGRAMS)

all: $(TARGETS)

$(PROGRAMS) $(TEST_PROGRAMS): $(EXTERNALS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/

clean:
	rm -rf $(PROGRAMS) $(TEST_PROGRAMS) *.o

test: all

.PHONY: all clean install test
