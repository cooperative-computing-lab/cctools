include ../../config.mk
include ../../rules.mk

PROGRAMS = poncho_package_analyze poncho_package_create poncho_package_run

all:

clean:
	rm -f $(CCTOOLS_INSTALL_DIR)/bin/poncho_package_analyze 
	rm -f $(CCTOOLS_INSTALL_DIR)/bin/poncho_package_run 
	rm -f $(CCTOOLS_INSTALL_DIR)/bin/poncho_package_create 

test:

install:
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/


.PHONY: all clean install test
