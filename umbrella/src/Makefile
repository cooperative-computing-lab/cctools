include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES =
LIBRARIES =
OBJECTS =
PROGRAMS = umbrella
SCRIPTS =
TARGETS = $(PROGRAMS) $(LIBRARIES) umbrella_helper.html

all: $(TARGETS)

umbrella: umbrella.py
	sed 's%CCTOOLS_VERSION%${CCTOOLS_VERSION}%' < umbrella.py > umbrella
	chmod 755 umbrella

umbrella_helper.html: umbrella.py
	pydoc -w umbrella
	mv umbrella.html umbrella_helper.html

clean:
	rm -f $(OBJECTS) $(TARGETS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	cp umbrella_helper.html $(CCTOOLS_INSTALL_DIR)/doc/

test: all

.PHONY: all clean install test
