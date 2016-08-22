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
	# pydoc2/pydoc creates a html file, umbrella.html.
	if which pydoc2; then pydoc2 -w umbrella; elif which pydoc; then pydoc -w umbrella; fi
	if [ -f umbrella.html ]; then mv umbrella.html umbrella_helper.html; fi

clean:
	rm -f $(OBJECTS) $(TARGETS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	if [ -f umbrella_helper.html ]; then cp umbrella_helper.html $(CCTOOLS_INSTALL_DIR)/doc/; fi

test: all

.PHONY: all clean install test
