include ../../../../../config.mk
include ../../../../../rules.mk

PROJECT_NAME := dagvine
SOURCE_DIR   := $(CCTOOLS_HOME)/taskvine/src/graph/$(PROJECT_NAME)/executor
BUILD_DIR    := ../build
MODULE_DIR   := $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/$(PROJECT_NAME)/executor
MODULE_NAME  := graph_capi

SOURCES   := node.c graph.c executor.c
OBJECTS   := $(SOURCES:%.c=$(BUILD_DIR)/%.o)
SWIG_WRAP := $(BUILD_DIR)/graph_wrap.c
WRAP_OBJ  := $(BUILD_DIR)/graph_wrap.o
PYMODULE  := $(BUILD_DIR)/_$(MODULE_NAME).$(CCTOOLS_DYNAMIC_SUFFIX)

LOCAL_LINKAGE += $(CCTOOLS_HOME)/taskvine/src/manager/libtaskvine.a $(CCTOOLS_HOME)/dttools/src/libdttools.a
LOCAL_CCFLAGS += -I $(CCTOOLS_HOME)/taskvine/src/manager -I $(SOURCE_DIR)

.PHONY: all install clean lint format test

all: $(PYMODULE)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(SWIG_WRAP): graph.i graph.h executor.h | $(BUILD_DIR)
	$(CCTOOLS_SWIG) -python -threads -relativeimport \
		-I$(CCTOOLS_HOME)/taskvine/src/manager \
		-I$(CCTOOLS_HOME)/dttools/src \
		-I$(CCTOOLS_HOME)/taskvine/src/graph/$(PROJECT_NAME) \
		-I$(SOURCE_DIR) \
		-outdir $(BUILD_DIR) -o $@ $<

$(WRAP_OBJ): $(SWIG_WRAP)
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $(CCTOOLS_PYTHON3_CCFLAGS) -w -fPIC -DNDEBUG $<

$(BUILD_DIR)/%.o: %.c graph.h executor.h node.h | $(BUILD_DIR)
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) -fPIC -DNDEBUG $<

$(PYMODULE): $(WRAP_OBJ) $(OBJECTS)
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_DYNAMIC_FLAG) $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_PYTHON3_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/graph/$(PROJECT_NAME)/include
	cp $(CCTOOLS_HOME)/taskvine/src/manager/taskvine.h $(CCTOOLS_INSTALL_DIR)/graph/$(PROJECT_NAME)/include/
	mkdir -p $(MODULE_DIR)
	cp $(PYMODULE) $(MODULE_DIR)
	cp $(BUILD_DIR)/$(MODULE_NAME).py $(MODULE_DIR)
	cp $(SOURCE_DIR)/__init__.py $(MODULE_DIR)
	cp $(SOURCE_DIR)/graph.py $(MODULE_DIR)

clean:
	rm -f *.o
	rm -rf __pycache__
	rm -rf $(BUILD_DIR)

lint:
	clang-format -Werror --dry-run --style='file:$(CCTOOLS_HOME)/.clang-format' $(SOURCE_DIR)/*.[ch]

format:
	clang-format -i $(SOURCE_DIR)/*.[ch]

test:
