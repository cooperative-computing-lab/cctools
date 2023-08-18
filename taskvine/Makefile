include ../config.mk
include ../rules.mk

PHONY_TARGETS ?= src
TARGETS ?= $(PHONY_TARGETS)

all: $(TARGETS)

$(TARGETS):
	@$(MAKE) -C $@

CLEAN_TARGETS = $(TARGETS:%=clean-%)
$(CLEAN_TARGETS):
	@$(MAKE) -C $(@:clean-%=%) clean
clean: $(CLEAN_TARGETS)

INSTALL_TARGETS = $(TARGETS:%=install-%)
$(INSTALL_TARGETS):
	@$(MAKE) -C $(@:install-%=%) install
install: $(INSTALL_TARGETS)

TEST_TARGETS = $(TARGETS:%=test-%)
$(TEST_TARGETS):
	@$(MAKE) -C $(@:test-%=%) test
test: $(TEST_TARGETS)

lint:
	@$(MAKE) -C src lint

format:
	@$(MAKE) -C src format

.PHONY: $(PHONY_TARGETS) $(CLEAN_TARGETS) $(INSTALL_TARGETS) $(TEST_TARGETS) all clean install test lint format
