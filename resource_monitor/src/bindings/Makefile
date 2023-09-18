include ../../../config.mk
include ../../../rules.mk

# List of errors flake8 should ignore when linting
NDCCTOOLS_FLAKE8_IGNORE_ERRORS = "F401,E251,E306,E226,E231,E241,E126,E123,E501,E266,E301,E302,E303,E305,W391,W293,W291,E128,E122,E275,E225"
# List of files flake8 should ignore when linting.
NDCCTOOLS_FLAKE8_IGNORE_FILES = "resource_monitor.bindings.py"

all clean install test: $(CCTOOLS_SWIG_RMONITOR_BINDINGS)

$(CCTOOLS_SWIG_RMONITOR_BINDINGS):
	@$(MAKE) -C $@ $(MAKECMDGOALS)

lint:
	flake8 --ignore=${NDCCTOOLS_FLAKE8_IGNORE_ERRORS} --exclude=${NDCCTOOLS_FLAKE8_IGNORE_FILES} .


test: all

.PHONY: all install clean test $(CCTOOLS_SWIG_RMONITOR_BINDINGS)
