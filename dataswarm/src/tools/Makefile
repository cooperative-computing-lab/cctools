include ../../../config.mk
include ../../../rules.mk

LIBDTTOOLS = ${CCTOOLS_HOME}/dttools/src/libdttools.a
EXTERNALS = $(LIBDTTOOLS) ../manager/libwork_queue.a
EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a

PROGRAMS = work_queue_status work_queue_server
TEST_PROGRAMS = work_queue_example work_queue_test work_queue_test_watch work_queue_priority_test work_queue_example_json
TARGETS = $(PROGRAMS) $(TEST_PROGRAMS)

all: $(TARGETS)

work_queue_test work_queue_test_watch: work_queue_test_main.o

$(PROGRAMS) $(TEST_PROGRAMS): $(EXTERNALS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	cp work_queue_example.c $(CCTOOLS_INSTALL_DIR)/doc/

clean:
	rm -rf $(PROGRAMS) $(TEST_PROGRAMS) *.o

test: all

.PHONY: all clean install test
