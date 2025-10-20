include ../../../../config.mk
include ../../../../rules.mk

LOCAL_LINKAGE+=${CCTOOLS_HOME}/taskvine/src/manager/libtaskvine.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_CCFLAGS+=-I ${CCTOOLS_HOME}/taskvine/src/manager


DAGVINE_SOURCE_DIR = $(CCTOOLS_HOME)/taskvine/src/graph/dagvine
DAGVINE_MODULE_DIR = $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/dagvine


SOURCES = strategic_orchestration_node.c strategic_orchestration_graph.c
OBJECTS = $(SOURCES:%.c=%.o)

SWIG_I = strategic_orchestration_graph.i

# put SWIG generated sources and Python extension artifacts into build/ directory
BUILD_DIR := build
SWIG_WRAP = $(BUILD_DIR)/strategic_orchestration_graph_wrap.c
WRAP_OBJ = $(BUILD_DIR)/strategic_orchestration_graph_wrap.o
PYMODULE = $(BUILD_DIR)/_capi.$(CCTOOLS_DYNAMIC_SUFFIX)

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

$(SWIG_WRAP): $(SWIG_I) strategic_orchestration_graph.h | $(BUILD_DIR)
	$(CCTOOLS_SWIG) -python -threads -relativeimport -I$(CCTOOLS_HOME)/taskvine/src/manager -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/taskvine/src/graph/dagvine -outdir $(BUILD_DIR) -o $@ $<

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
	mkdir -p $(CCTOOLS_INSTALL_DIR)/graph/dagvine/include
	cp ${CCTOOLS_HOME}/taskvine/src/manager/taskvine.h $(CCTOOLS_INSTALL_DIR)/graph/dagvine/include/
	mkdir -p $(DAGVINE_MODULE_DIR)
	cp $(PYMODULE) $(DAGVINE_MODULE_DIR)
	cp $(BUILD_DIR)/capi.py $(DAGVINE_MODULE_DIR)
	# also install pure Python sources for the dagvine package
	cp $(DAGVINE_SOURCE_DIR)/*.py $(DAGVINE_MODULE_DIR)

clean:
	rm -rf $(PROGRAMS) $(OBJECTS) *.o $(BUILD_DIR) $(PYMODULE) capi.py

lint:
	if ( ! clang-format -Werror --dry-run --style='file:${CCTOOLS_HOME}/.clang-format' $(DAGVINE_SOURCE_DIR)/*.c $(DAGVINE_SOURCE_DIR)/*.h);\
	then\
		echo  "========================================================";\
		echo  "NOTICE: Run \`make format\` to format your latest changes.";\
		echo  "========================================================";\
		exit 1;\
	fi
	flake8 --ignore=$(CCTOOLS_FLAKE8_IGNORE_ERRORS) \
		--exclude=$(CCTOOLS_FLAKE8_IGNORE_FILES),$(BUILD_DIR) \
		$(DAGVINE_SOURCE_DIR)/

format:
	clang-format -i $(DAGVINE_SOURCE_DIR)/*.c $(DAGVINE_SOURCE_DIR)/*.h

.PHONY: all clean install test lint format


