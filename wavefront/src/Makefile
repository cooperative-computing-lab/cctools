include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = ../../work_queue/src/libwork_queue.a ../../chirp/src/libchirp.a ../../dttools/src/libdttools.a
PROGRAMS = wavefront wavefront_master
TARGETS = $(PROGRAMS)

all: $(PROGRAMS)
$(PROGRAMS): $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(PROGRAMS)

test:

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/
