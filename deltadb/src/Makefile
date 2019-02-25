include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
LIBRARIES = libdeltadb.a
OBJECTS = $(SOURCES:%.c=%.o)
PROGRAMS = deltadb_query deltadb_upgrade_log
SCRIPTS =
SOURCES = deltadb_stream.c deltadb_reduction.c
TARGETS = $(LIBRARIES) $(PROGRAMS)

all: $(TARGETS)

libdeltadb.a: $(OBJECTS)

$(PROGRAMS): $(LIBRARIES) $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(OBJECTS) $(TARGETS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin

test: all

.PHONY: all clean install test
