include ../../../../config.mk
include ../../../../rules.mk

LOCAL_LINKAGE+=${CCTOOLS_HOME}/taskvine/src/manager/libtaskvine.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_CCFLAGS+=-I ${CCTOOLS_HOME}/taskvine/src/manager

SOURCES = vine_task_node.c vine_task_graph.c
OBJECTS = $(SOURCES:%.c=%.o)

SWIG_I = vine_task_graph.i
SWIG_WRAP = vine_task_graph_wrap.c
PYMODULE = _cdagvine.$(CCTOOLS_DYNAMIC_SUFFIX)

LIBRARIES = libdagvine.a
PROGRAMS =
SCRIPTS =
TARGETS = $(LIBRARIES) $(PYMODULE) $(PROGRAMS)

all: $(TARGETS)

libdagvine.a: $(OBJECTS)

$(PROGRAMS): $(EXTERNALS)

$(SWIG_WRAP): $(SWIG_I) vine_task_graph.h
	$(CCTOOLS_SWIG) -python -threads -relativeimport -I$(CCTOOLS_HOME)/taskvine/src/manager -I$(CCTOOLS_HOME)/dttools/src -o $@ $<

# Build Python module
vine_task_graph_wrap.o: $(SWIG_WRAP)
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(CCTOOLS_PYTHON3_CCFLAGS) $(LOCAL_CCFLAGS) $<

$(PYMODULE): vine_task_graph_wrap.o libdagvine.a
ifeq ($(CCTOOLS_STATIC),1)
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_DYNAMIC_FLAG) $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_PYTHON3_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)
else
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_DYNAMIC_FLAG) $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_PYTHON3_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)
endif



install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/graph/dagvine/include
	cp ${CCTOOLS_HOME}/taskvine/src/manager/taskvine.h $(CCTOOLS_INSTALL_DIR)/graph/dagvine/include/
	mkdir -p $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/dagvine
	cp $(PYMODULE) $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/dagvine/
	cp cdagvine.py $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/dagvine/

clean:
	rm -rf $(PROGRAMS) $(OBJECTS) *.o $(SWIG_WRAP) vine_task_graph_wrap.o $(PYMODULE) cdagvine.py

test: all

lint:

format:

.PHONY: all clean install test lint format


