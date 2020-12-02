include ../config.mk
include ../rules.mk

PHONY_TARGETS ?= common manager worker
TARGETS ?= $(PHONY_TARGETS)

all: $(TARGETS)

common:
	@$(MAKE) -C $@

manager: common
	@$(MAKE) -C $@

worker: common
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

.PHONY: $(PHONY_TARGETS) $(CLEAN_TARGETS) $(INSTALL_TARGETS) $(TEST_TARGETS) clean install test
