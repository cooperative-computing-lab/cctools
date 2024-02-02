include ../../../config.mk
include ../../../rules.mk

CCTOOLS_FLAKE8_IGNORE_FILES = "resource_monitor.bindings.py,cresource_monitor.py"


all clean install test: $(CCTOOLS_SWIG_RMONITOR_BINDINGS)

$(CCTOOLS_SWIG_RMONITOR_BINDINGS):
	@$(MAKE) -C $@ $(MAKECMDGOALS)

lint:
	flake8 --ignore=$(CCTOOLS_FLAKE8_IGNORE_ERRORS) --exclude=$(CCTOOLS_FLAKE8_IGNORE_FILES) .


test: all

.PHONY: all install clean test $(CCTOOLS_SWIG_RMONITOR_BINDINGS)
