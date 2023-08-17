include ../../../config.mk
include ../../../rules.mk

all clean install test: $(CCTOOLS_SWIG_TASKVINE_BINDINGS)

$(CCTOOLS_SWIG_TASKVINE_BINDINGS):
	@$(MAKE) -C $@ $(MAKECMDGOALS)

test: all

lint:
	@$(MAKE) -C python3 lint

.PHONY: all install clean test lint $(CCTOOLS_SWIG_TASKVINE_BINDINGS)
