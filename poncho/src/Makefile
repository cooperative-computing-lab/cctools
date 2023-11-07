include ../../config.mk
include ../../rules.mk

PROGRAMS = poncho_package_analyze poncho_package_create poncho_package_run poncho_package_serverize
ALIASES = python_package_analyze python_package_create python_package_run 
# List of errors flake8 should ignore when linting
NDCCTOOLS_FLAKE8_IGNORE_ERRORS = "E306,E226,E231,E241,E126,E123,E501,E266,E301,E302,E303,E305,W391,W293,W291,E128,E122,E275,E225"
# List of files flake8 should ignore when linting.
NDCCTOOLS_FLAKE8_IGNORE_FILES = "package_serverize.py poncho_test.py"


all:

clean:

lint:
	flake8 --ignore=${NDCCTOOLS_FLAKE8_IGNORE_ERRORS} --exclude=${NDCCTOOLS_FLAKE8_IGNORE_FILES} .

test:

install:
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(ALIASES) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_PYTHON3_PATH)/ndcctools/poncho
	cp -a poncho $(CCTOOLS_PYTHON3_PATH)/ndcctools


.PHONY: all clean install test
