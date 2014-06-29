include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = ../../chirp/src/libchirp.a ../../dttools/src/libdttools.a
OBJECTS = ragged_array.o
PROGRAMS = allpairs allpairs_status allpairs_wait allpairs_cleanup
SCRIPTS = allpairs_wrapper.sh
TARGETS = $(PROGRAMS)

all: $(TARGETS)

$(PROGRAMS): $(EXTERNAL_DEPENDENCIES)
allpairs: $(OBJECTS)

test:

clean:
	rm -f $(OBJECTS) $(TARGETS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
