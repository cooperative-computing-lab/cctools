include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
LIBRARIES = libdeltadb.a
OBJECTS = $(SOURCES:%.c=%.o)
PROGRAMS = deltadb_collect deltadb_select_static deltadb_select_dynamic deltadb_select_complete deltadb_project deltadb_reduce_temporal deltadb_reduce_spatial deltadb_pivot
SCRIPTS = deltadb_collect deltadb_select_static deltadb_select_dynamic deltadb_select_complete deltadb_project deltadb_reduce_temporal deltadb_reduce_spatial deltadb_pivot
SOURCES = deltadb_reduction.c
TARGETS = $(LIBRARIES) $(PROGRAMS)

all: $(TARGETS)

libdeltadb.a: $(OBJECTS)
$(PROGRAMS): $(LIBRARIES) $(EXTERNAL_DEPENDENCIES)

test: all

clean:
	rm -f $(OBJECTS) $(TARGETS)

uninstall:
	for s in $(SCRIPTS); do rm -f $(CCTOOLS_INSTALL_DIR)/bin/$$s; done

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	for s in $(SCRIPTS); do cat $$s | sed "s:CCTOOLS_VERSION:$(CCTOOLS_VERSION):g" | sed "s:CCTOOLS_RELEASEDATE:$(CCTOOLS_RELEASEDATE):g" > $(CCTOOLS_INSTALL_DIR)/bin/$$s; chmod 755 $(CCTOOLS_INSTALL_DIR)/bin/$$s; done

.PHONY: all clean install test uninstall
