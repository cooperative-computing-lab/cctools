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

LINT_TARGETS = $(TARGETS:%=lint-%)
$(LINT_TARGETS):
	@$(MAKE) -C $(@:lint-%=%) lint
lint: $(LINT_TARGETS)

FORMAT_TARGETS = $(TARGETS:%=format-%)
$(FORMAT_TARGETS):
	@$(MAKE) -C $(@:format-%=%) format
format: $(FORMAT_TARGETS)

INSTALL_TARGETS = $(TARGETS:%=install-%)
$(INSTALL_TARGETS):
	@$(MAKE) -C $(@:install-%=%) install
install: $(INSTALL_TARGETS)

TEST_TARGETS = $(TARGETS:%=test-%)
$(TEST_TARGETS):
	@$(MAKE) -C $(@:test-%=%) test
test: $(TEST_TARGETS)

.PHONY: $(PHONY_TARGETS) $(CLEAN_TARGETS) $(INSTALL_TARGETS) $(TEST_TARGETS) all clean install test
