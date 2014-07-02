include ../../config.mk
include ../../rules.mk

LOCAL_LINKAGE = -lpthread

EXTERNAL_DEPENDENCIES = ../../sand/src/libsandtools.a ../../work_queue/src/libwork_queue.a ../../dttools/src/libdttools.a
OBJECTS = allpairs_compare.o
PROGRAMS = allpairs_multicore allpairs_master
TARGETS = $(PROGRAMS)

all: $(TARGETS)

$(PROGRAMS): $(OBJECTS) $(EXTERNAL_DEPENDENCIES)

test:

clean:
	rm -f $(OBJECTS) $(TARGETS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/
