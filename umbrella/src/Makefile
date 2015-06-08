include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = 
LIBRARIES = 
OBJECTS = 
PROGRAMS = 
SCRIPTS = umbrella cctools_version.py
TARGETS = $(PROGRAMS) $(LIBRARIES)

all: $(TARGETS)

clean:
	rm -f $(OBJECTS) $(TARGETS)

install: all
	m4 -DCCTOOLS_VERSION='${CCTOOLS_VERSION}' < cctools_version > cctools_version.py
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	chmod 755 $(SCRIPTS)
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/

test: all

.PHONY: all clean install test
