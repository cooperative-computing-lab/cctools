include ../../../config.mk
include ../../../rules.mk

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
	@if $(CCTOOLS_PYDOC) -w umbrella > /dev/null 2>&1; then mv umbrella.html umbrella_helper.html; else echo Could not create $@; fi

clean:
	rm -f $(OBJECTS) $(TARGETS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc/cctools/umbrella
	if [ -f umbrella_helper.html ]; then cp umbrella_helper.html $(CCTOOLS_INSTALL_DIR)/doc/cctools/umbrella; fi

test: all

.PHONY: all clean install test
