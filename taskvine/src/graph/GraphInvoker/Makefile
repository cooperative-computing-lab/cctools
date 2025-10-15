include ../../../../config.mk
include ../../../../rules.mk

LOCAL_LINKAGE+=${CCTOOLS_HOME}/taskvine/src/manager/libtaskvine.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_CCFLAGS+=-I ${CCTOOLS_HOME}/taskvine/src/manager

SOURCES = vine_task_node.c vine_task_graph.c
OBJECTS = $(SOURCES:%.c=%.o)

LIBRARIES = libgraphinvoker.a
PROGRAMS =
SCRIPTS =
TARGETS = $(LIBRARIES) $(PROGRAMS)

all: $(TARGETS)

libgraphinvoker.a: $(OBJECTS)

$(PROGRAMS): $(EXTERNALS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/graph/GraphInvoker/include
	cp ${CCTOOLS_HOME}/taskvine/src/manager/taskvine.h $(CCTOOLS_INSTALL_DIR)/graph/GraphInvoker/include/

clean:
	rm -rf $(PROGRAMS) $(OBJECTS) *.o

test: all

lint:

format:

.PHONY: all clean install test lint format


