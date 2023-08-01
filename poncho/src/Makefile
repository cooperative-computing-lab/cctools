include ../../config.mk
include ../../rules.mk

PROGRAMS = poncho_package_analyze poncho_package_create poncho_package_run poncho_package_serverize
ALIASES = python_package_analyze python_package_create python_package_run 

all:

clean:

test:

install:
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(ALIASES) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_PYTHON3_PATH)/ndcctools/poncho
	cp -a poncho $(CCTOOLS_PYTHON3_PATH)/ndcctools


.PHONY: all clean install test
