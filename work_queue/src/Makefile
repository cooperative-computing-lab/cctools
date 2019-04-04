include ../../config.mk
include ../../rules.mk

# Note that this list of sources only contains
# the items that go into libwork_queue.a, to
# avoid polluting user programs with our internal code.

SOURCES_LIBRARY = \
	work_queue.c \
	work_queue_catalog.c \
	work_queue_resources.c

SOURCES_WORKER = \
	work_queue_process.o \
	work_queue_watcher.o

PUBLIC_HEADERS = work_queue.h

LIBDTTOOLS = ${CCTOOLS_HOME}/dttools/src/libdttools.a
EXTERNALS = $(LIBDTTOOLS)
EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
LIBRARIES = libwork_queue.a
OBJECTS = $(OBJECTS_LIBRARY) $(OBJECTS_WORKER) work_queue_test_main.o
OBJECTS_LIBRARY = $(SOURCES_LIBRARY:%.c=%.o)
OBJECTS_WORKER = $(SOURCES_WORKER:%.c=%.o)
PROGRAMS = work_queue_worker work_queue_status work_queue_example
PUBLIC_HEADERS = work_queue.h
SCRIPTS = work_queue_submit_common condor_submit_workers sge_submit_workers torque_submit_workers pbs_submit_workers slurm_submit_workers work_queue_graph_log
TEST_PROGRAMS = work_queue_example work_queue_test work_queue_test_watch work_queue_priority_test
TARGETS = $(LIBRARIES) $(PROGRAMS) $(TEST_PROGRAMS) sge_submit_workers bindings

all: $(TARGETS)

libwork_queue.a: $(OBJECTS_LIBRARY)
work_queue_test work_queue_test_watch: work_queue_test_main.o
work_queue_worker: $(OBJECTS_WORKER)
$(PROGRAMS) $(TEST_PROGRAMS): libwork_queue.a $(EXTERNAL_DEPENDENCIES)

bindings: libwork_queue.a work_queue.i $(EXTERNAL_DEPENDENCIES)
	@$(MAKE) -C bindings
	
sge_submit_workers: sge_submit_workers_common
	sed -e "s/SGE_CUSTOM_PARAMETERS/${CCTOOLS_SGE_PARAMETERS}/" $< > $@
	chmod 755 $@

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	chmod 755 $(SCRIPTS)
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	cp work_queue_example.c $(CCTOOLS_INSTALL_DIR)/doc/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(PUBLIC_HEADERS) $(CCTOOLS_INSTALL_DIR)/include/cctools/
	@$(MAKE) -C bindings install

clean:
	rm -rf $(OBJECTS) $(LIBRARIES) $(PROGRAMS) $(TEST_PROGRAMS) *.o sge_submit_workers
	@$(MAKE) -C bindings clean

test: all

.PHONY: all clean install test bindings
