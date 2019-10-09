include ../../../config.mk
include ../../../rules.mk

all clean install test: $(CCTOOLS_SWIG_RMONITOR_BINDINGS)

$(CCTOOLS_SWIG_RMONITOR_BINDINGS):
	@$(MAKE) -C $@ $(MAKECMDGOALS)


test: all

.PHONY: all install clean test $(CCTOOLS_SWIG_RMONITOR_BINDINGS)
