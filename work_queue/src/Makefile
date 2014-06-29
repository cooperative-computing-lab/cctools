include ../../config.mk
include ../../rules.mk

SOURCES = \
	batch_job.c \
	batch_job_chirp.c \
	batch_job_cluster.c \
	batch_job_condor.c \
	batch_job_hadoop.c \
	batch_job_local.c \
	batch_job_mpi_queue.c \
	batch_job_work_queue.c \
	work_queue.c \
	work_queue_catalog.c \
	work_queue_example.c \
	work_queue_pool.c \
	work_queue_resources.c \
	work_queue_status.c \
	work_queue_worker.c \
	work_queue_workload_simulator.c

EXTERNAL_DEPENDENCIES = ../../chirp/src/libchirp.a ../../dttools/src/libdttools.a
LIBRARIES = libwork_queue.a
OBJECTS = $(SOURCES:%.c=%.o)
PROGRAMS = work_queue_worker work_queue_status work_queue_example work_queue_pool
PUBLIC_HEADERS = batch_job.h work_queue.h
SCRIPTS = wq_submit_workers.common condor_submit_workers sge_submit_workers torque_submit_workers pbs_submit_workers ec2_submit_workers ec2_remove_workers slurm_submit_workers work_queue_graph_log
TEST_PROGRAMS = work_queue_example work_queue_workload_simulator
TARGETS = $(LIBRARIES) $(PROGRAMS) $(TEST_PROGRAMS) bindings

ifeq ($(CCTOOLS_USE_MPI),true)
	PROGRAMS += mpi_queue_worker
endif

all: $(TARGETS)

libwork_queue.a: $(OBJECTS)
$(PROGRAMS) $(TEST_PROGRAMS) bindings: libwork_queue.a $(EXTERNAL_DEPENDENCIES)

mpi_queue_worker: mpi_queue_worker.o
	$(CCTOOLS_MPI_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE)

bindings: $(CCTOOLS_WORK_QUEUE_BINDINGS)
$(CCTOOLS_WORK_QUEUE_BINDINGS): libwork_queue.a work_queue.i
	@$(MAKE) -C $@

CCTOOLS_WORK_QUEUE_BINDINGS_INSTALL = $(CCTOOLS_WORK_QUEUE_BINDINGS:%=install-%)
install-bindings: $(CCTOOLS_WORK_QUEUE_BINDINGS_INSTALL)
$(CCTOOLS_WORK_QUEUE_BINDINGS_INSTALL): $(CCTOOLS_WORK_QUEUE_BINDINGS)
	@$(MAKE) -C $(@:install-%=%) install

CCTOOLS_WORK_QUEUE_BINDINGS_CLEAN = $(CCTOOLS_WORK_QUEUE_BINDINGS:%=clean-%)
clean-bindings: $(CCTOOLS_WORK_QUEUE_BINDINGS_CLEAN)
$(CCTOOLS_WORK_QUEUE_BINDINGS_CLEAN):
	@$(MAKE) -C $(@:clean-%=%) clean

install: all install-bindings
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	chmod 755 $(SCRIPTS)
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	cp work_queue_example.c $(CCTOOLS_INSTALL_DIR)/doc/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(PUBLIC_HEADERS) $(CCTOOLS_INSTALL_DIR)/include/cctools/

test:

clean: clean-bindings
	rm -rf $(OBJECTS) $(LIBRARIES) $(PROGRAMS) $(TEST_PROGRAMS)

.PHONY: all clean install test $(CCTOOLS_WORK_QUEUE_BINDINGS) bindings $(CCTOOLS_WORK_QUEUE_BINDINGS_INSTALL) install-bindings $(CCTOOLS_WORK_QUEUE_BINDINGS_CLEAN) clean-bindings
