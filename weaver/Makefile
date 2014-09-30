include ../config.mk
include ../rules.mk

PYTHON_INSTALL_DIR=$(CCTOOLS_INSTALL_DIR)/lib/python$(CCTOOLS_PYTHON_VERSION)/site-packages

PHONY_TARGETS ?= weaver
TARGETS ?= weaver

all: $(TARGETS)

weaver:
	$(CCTOOLS_PYTHON) setup.py build

CLEAN_TARGETS = $(TARGETS:%=clean-%)
$(CLEAN_TARGETS):
	$(CCTOOLS_PYTHON) setup.py clean
clean: $(CLEAN_TARGETS)

INSTALL_TARGETS = $(TARGETS:%=install-%)
$(INSTALL_TARGETS):
	$(CCTOOLS_PYTHON) setup.py install_scripts -d $(CCTOOLS_INSTALL_DIR)/bin
	$(CCTOOLS_PYTHON) setup.py install_lib     -d $(PYTHON_INSTALL_DIR)
install: $(INSTALL_TARGETS)

TEST_TARGETS = $(TARGETS:%=test-%)
$(TEST_TARGETS):
	$(CCTOOLS_PYTHON) setup.py test
test: $(TEST_TARGETS)

.PHONY: $(PHONY_TARGETS) $(CLEAN_TARGETS) $(INSTALL_TARGETS) $(TEST_TARGETS) all clean install test
