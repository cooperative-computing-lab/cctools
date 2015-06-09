include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES =
LIBRARIES =
OBJECTS =
PROGRAMS = umbrella
SCRIPTS =
TARGETS = $(PROGRAMS) $(LIBRARIES)

all: $(TARGETS)

umbrella: umbrella.py
	sed 's%CCTOOLS_VERSION%${CCTOOLS_VERSION}%' < umbrella.py > umbrella
	chmod 755 umbrella

clean:
	rm -f $(OBJECTS) $(TARGETS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/

test: all

.PHONY: all clean install test
