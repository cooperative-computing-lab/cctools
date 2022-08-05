include ../../../config.mk
include ../../../rules.mk

LIBDTTOOLS = ${CCTOOLS_HOME}/dttools/src/libdttools.a
EXTERNALS = $(LIBDTTOOLS) ../manager/libwork_queue.a
EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a

PROGRAMS = ds_status ds_server
TEST_PROGRAMS = ds_example ds_test ds_test_watch ds_priority_test ds_example_json
TARGETS = $(PROGRAMS) $(TEST_PROGRAMS)

all: $(TARGETS)

ds_test ds_test_watch: ds_test_main.o

$(PROGRAMS) $(TEST_PROGRAMS): $(EXTERNALS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	cp ds_example.c $(CCTOOLS_INSTALL_DIR)/doc/

clean:
	rm -rf $(PROGRAMS) $(TEST_PROGRAMS) *.o

test: all

.PHONY: all clean install test
