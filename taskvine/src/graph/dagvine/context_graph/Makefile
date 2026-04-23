include ../../../../../config.mk
include ../../../../../rules.mk

PROJECT_NAME = dagvine

SOURCE_DIR = $(CCTOOLS_HOME)/taskvine/src/graph/$(PROJECT_NAME)/context_graph
MODULE_ROOT = $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/$(PROJECT_NAME)
MODULE_DIR = $(MODULE_ROOT)/context_graph

PY_SOURCES = $(wildcard $(SOURCE_DIR)/*.py)

.PHONY: all install clean lint format

all:
	@true

install:
	mkdir -p $(MODULE_DIR)
	cp $(PY_SOURCES) $(MODULE_DIR)

clean:
	rm -rf __pycache__

lint:
	flake8 --ignore=$(CCTOOLS_FLAKE8_IGNORE_ERRORS) \
		--exclude=$(CCTOOLS_FLAKE8_IGNORE_FILES) \
		$(SOURCE_DIR)/

format:
	@true

