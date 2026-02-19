include ../../../../../config.mk
include ../../../../../rules.mk

PROJECT_NAME = dagvine

LOCAL_LINKAGE+=${CCTOOLS_HOME}/taskvine/src/manager/libtaskvine.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_CCFLAGS+=-I ${CCTOOLS_HOME}/taskvine/src/manager
LOCAL_CCFLAGS+=-I ${CCTOOLS_HOME}/taskvine/src/graph/$(PROJECT_NAME)/vine_graph

SOURCE_DIR = $(CCTOOLS_HOME)/taskvine/src/graph/$(PROJECT_NAME)/vine_graph
MODULE_ROOT = $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/$(PROJECT_NAME)
MODULE_DIR = $(MODULE_ROOT)/vine_graph

SOURCES = vine_node.c vine_graph.c
OBJECTS = $(SOURCES:%.c=%.o)

BUILD_DIR := ../build

# put SWIG generated sources and Python extension artifacts into ../build/
SWIG_I = vine_graph.i

WRAP_NAME = vine_graph_wrap
MODULE_NAME = vine_graph_capi

SWIG_WRAP = $(BUILD_DIR)/$(WRAP_NAME).c
WRAP_OBJ = $(BUILD_DIR)/$(WRAP_NAME).o
PYMODULE = $(BUILD_DIR)/_$(MODULE_NAME).$(CCTOOLS_DYNAMIC_SUFFIX)

LIBRARIES =
PYDEPS = $(WRAP_OBJ) $(OBJECTS)
PYLINK_INPUT = $(WRAP_OBJ) $(OBJECTS)
PROGRAMS =
SCRIPTS =
TARGETS = $(LIBRARIES) $(PYMODULE) $(PROGRAMS)

.PHONY: all install clean lint format

all: $(TARGETS)

$(PROGRAMS): $(EXTERNALS)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(SWIG_WRAP): $(SWIG_I) vine_graph.h | $(BUILD_DIR)
	$(CCTOOLS_SWIG) -python -threads -relativeimport \
		-I$(CCTOOLS_HOME)/taskvine/src/manager \
		-I$(CCTOOLS_HOME)/dttools/src \
		-I$(CCTOOLS_HOME)/taskvine/src/graph/$(PROJECT_NAME) \
		-I$(CCTOOLS_HOME)/taskvine/src/graph/$(PROJECT_NAME)/vine_graph \
		-outdir $(BUILD_DIR) -o $@ $<

# Build Python module (mimic bindings: silence SWIG warnings and build PIC)
$(WRAP_OBJ): $(SWIG_WRAP)
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $(CCTOOLS_PYTHON3_CCFLAGS) -w -fPIC -DNDEBUG $<

$(PYMODULE): $(PYDEPS)
ifeq ($(CCTOOLS_STATIC),1)
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_DYNAMIC_FLAG) $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $(PYLINK_INPUT) $(LOCAL_LINKAGE) $(CCTOOLS_PYTHON3_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)
else
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_DYNAMIC_FLAG) $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $(PYLINK_INPUT) $(LOCAL_LINKAGE) $(CCTOOLS_PYTHON3_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)
endif

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/graph/$(PROJECT_NAME)/include
	cp ${CCTOOLS_HOME}/taskvine/src/manager/taskvine.h $(CCTOOLS_INSTALL_DIR)/graph/$(PROJECT_NAME)/include/
	mkdir -p $(MODULE_DIR)
	cp $(PYMODULE) $(MODULE_DIR)
	cp $(BUILD_DIR)/$(MODULE_NAME).py $(MODULE_DIR)
	cp $(SOURCE_DIR)/__init__.py $(MODULE_DIR)
	cp $(SOURCE_DIR)/vine_graph_client.py $(MODULE_DIR)

clean:
	rm -f $(PROGRAMS) $(OBJECTS) $(WRAP_OBJ)
	rm -f $(PYMODULE) $(BUILD_DIR)/$(MODULE_NAME).py
	rm -rf $(BUILD_DIR)

lint:
	if ( ! clang-format -Werror --dry-run --style='file:${CCTOOLS_HOME}/.clang-format' $(SOURCE_DIR)/*.c $(SOURCE_DIR)/*.h); \
	then \
		echo  "========================================================"; \
		echo  "NOTICE: Run \`make format\` to format your latest changes."; \
		echo  "========================================================"; \
		exit 1; \
	fi

format:
	clang-format -i $(SOURCE_DIR)/*.c $(SOURCE_DIR)/*.h


