include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
LIBRARIES = libdeltadb.a
OBJECTS = $(SOURCES:%.c=%.o)
PROGRAMS = deltadb_query deltadb_upgrade_log catalog_server
SCRIPTS =
SOURCES = deltadb.c deltadb_query.c deltadb_stream.c deltadb_reduction.c
TARGETS = $(LIBRARIES) $(PROGRAMS)

all: $(TARGETS)

libdeltadb.a: $(OBJECTS)

deltadb_query: deltadb_query_main.o libdeltadb.a $(EXTERNAL_DEPENDENCIES)

deltadb_upgrade_log: deltadb_upgrade_log.o libdeltadb.a $(EXTERNAL_DEPENDENCIES)

catalog_server: catalog_server.o catalog_export.o libdeltadb.a $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(OBJECTS) $(TARGETS) *.o

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin

test: all

.PHONY: all clean install test
