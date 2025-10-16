include ../../../../config.mk
include ../../../../rules.mk

LOCAL_LINKAGE+=${CCTOOLS_HOME}/taskvine/src/manager/libtaskvine.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_CCFLAGS+=-I ${CCTOOLS_HOME}/taskvine/src/manager

 

SOURCES = vine_task_node.c vine_task_graph.c
OBJECTS = $(SOURCES:%.c=%.o)

SWIG_I = vine_task_graph.i

# put SWIG generated sources and Python extension artifacts into build/ directory
BUILD_DIR := build
SWIG_WRAP = $(BUILD_DIR)/vine_task_graph_wrap.c
WRAP_OBJ = $(BUILD_DIR)/vine_task_graph_wrap.o
PYMODULE = $(BUILD_DIR)/_cdagvine.$(CCTOOLS_DYNAMIC_SUFFIX)

LIBRARIES =
PYDEPS = $(WRAP_OBJ) $(OBJECTS)
PYLINK_INPUT = $(WRAP_OBJ) $(OBJECTS)
PROGRAMS =
SCRIPTS =
TARGETS = $(LIBRARIES) $(PYMODULE) $(PROGRAMS)

all: $(TARGETS)

 

$(PROGRAMS): $(EXTERNALS)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(SWIG_WRAP): $(SWIG_I) vine_task_graph.h | $(BUILD_DIR)
	$(CCTOOLS_SWIG) -python -threads -relativeimport -I$(CCTOOLS_HOME)/taskvine/src/manager -I$(CCTOOLS_HOME)/dttools/src -outdir $(BUILD_DIR) -o $@ $<

# Build Python module
$(WRAP_OBJ): $(SWIG_WRAP)
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(CCTOOLS_PYTHON3_CCFLAGS) $(LOCAL_CCFLAGS) -Wno-error=cast-function-type $<

$(PYMODULE): $(PYDEPS)
ifeq ($(CCTOOLS_STATIC),1)
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_DYNAMIC_FLAG) $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $(PYLINK_INPUT) $(LOCAL_LINKAGE) $(CCTOOLS_PYTHON3_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)
else
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_DYNAMIC_FLAG) $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $(PYLINK_INPUT) $(LOCAL_LINKAGE) $(CCTOOLS_PYTHON3_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)
endif



install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/graph/dagvine/include
	cp ${CCTOOLS_HOME}/taskvine/src/manager/taskvine.h $(CCTOOLS_INSTALL_DIR)/graph/dagvine/include/
	mkdir -p $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/dagvine
	cp $(PYMODULE) $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/dagvine/
	cp $(BUILD_DIR)/cdagvine.py $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/dagvine/
	# remove build artifacts after installation
	rm -rf $(BUILD_DIR)

clean:
	rm -rf $(PROGRAMS) $(OBJECTS) *.o $(BUILD_DIR) $(PYMODULE) cdagvine.py

test: all

lint:

format:

.PHONY: all clean install test lint format


