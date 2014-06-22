include ../Makefile.config
include ../Makefile.rules

PHONY_TARGETS ?= src
TARGETS ?= $(PHONY_TARGETS)

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

.PHONY: $(PHONY_TARGETS) all clean install test
