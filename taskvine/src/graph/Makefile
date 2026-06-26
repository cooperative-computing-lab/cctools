include ../../../config.mk
include ../../../rules.mk

CCTOOLS_DYNAMIC_SUFFIX = so

PROJECT_NAME := vine_graph
SOURCE_DIR   := $(CCTOOLS_HOME)/taskvine/src/graph
PY_MODULE_DIR := $(CCTOOLS_HOME)/taskvine/src/bindings/python3/ndcctools/taskvine/vine_graph
MODULE_DIR   := $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/vine_graph
TEST_MODULE_DIR := $(CCTOOLS_HOME)/test_support/python_modules/$(CCTOOLS_PYTHON_TEST_DIR)/ndcctools/taskvine/vine_graph
MODULE_NAME  := vine_graph_capi

SOURCES   := vine_graph_node.c vine_graph.c vine_graph_executor.c
OBJECTS   := $(SOURCES:%.c=%.o)
SWIG_WRAP := vine_graph_wrap.c
WRAP_OBJ  := vine_graph_wrap.o
PYMODULE  := _$(MODULE_NAME).$(CCTOOLS_DYNAMIC_SUFFIX)

LOCAL_LINKAGE += $(CCTOOLS_HOME)/taskvine/src/manager/libtaskvine.a $(CCTOOLS_HOME)/dttools/src/libdttools.a
LOCAL_CCFLAGS += -I $(CCTOOLS_HOME)/taskvine/src/manager -I $(SOURCE_DIR)

.PHONY: all install clean lint format test

all: $(PYMODULE)

$(SWIG_WRAP): vine_graph.i vine_graph.h vine_graph_executor.h
	$(CCTOOLS_SWIG) -python -threads -relativeimport \
		-I$(CCTOOLS_HOME)/taskvine/src/manager \
		-I$(CCTOOLS_HOME)/dttools/src \
		-I$(SOURCE_DIR) \
		-outdir $(SOURCE_DIR) -o $@ $<

$(WRAP_OBJ): $(SWIG_WRAP)
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $(CCTOOLS_PYTHON3_CCFLAGS) -w -fPIC -DNDEBUG $<

%.o: %.c vine_graph.h vine_graph_executor.h vine_graph_node.h
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) -fPIC -DNDEBUG $<

$(PYMODULE): $(WRAP_OBJ) $(OBJECTS)
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_DYNAMIC_FLAG) $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_PYTHON3_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/$(PROJECT_NAME)/include
	cp $(CCTOOLS_HOME)/taskvine/src/manager/taskvine.h $(CCTOOLS_INSTALL_DIR)/$(PROJECT_NAME)/include/
	mkdir -p $(MODULE_DIR)
	rm -f $(MODULE_DIR)/graph.py $(MODULE_DIR)/graph_capi.py $(MODULE_DIR)/_graph_capi.$(CCTOOLS_DYNAMIC_SUFFIX)
	rm -f $(MODULE_DIR)/vine_graph_capi.py $(MODULE_DIR)/_vine_graph_capi.$(CCTOOLS_DYNAMIC_SUFFIX)
	rm -f $(MODULE_DIR)/capi_bridge.py
	cp $(PYMODULE) $(MODULE_DIR)
	cp $(MODULE_NAME).py $(MODULE_DIR)
	cp $(PY_MODULE_DIR)/capi_bridge.py $(MODULE_DIR)
	mkdir -p $(TEST_MODULE_DIR)
	rm -f $(TEST_MODULE_DIR)/vine_graph_capi.py $(TEST_MODULE_DIR)/_vine_graph_capi.$(CCTOOLS_DYNAMIC_SUFFIX)
	rm -f $(TEST_MODULE_DIR)/capi_bridge.py
	cp $(PYMODULE) $(TEST_MODULE_DIR)
	cp $(MODULE_NAME).py $(TEST_MODULE_DIR)
	cp $(MODULE_DIR)/capi_bridge.py $(TEST_MODULE_DIR)

clean:
	rm -f *.o
	rm -rf __pycache__
	rm -f $(SWIG_WRAP) $(WRAP_OBJ) $(PYMODULE) $(MODULE_NAME).py

lint:
	clang-format -Werror --dry-run --style='file:$(CCTOOLS_HOME)/.clang-format' $(SOURCE_DIR)/*.[ch]

format:
	clang-format -i $(SOURCE_DIR)/*.[ch]

test:
