include ../../config.mk
include ../../rules.mk

LIBRARIES = libbatch_job.a

PROGRAMS = work_queue_factory work_queue_pool vine_factory
SCRIPTS = condor_chaos_monkey

DT_LIB=${CCTOOLS_HOME}/dttools/src/libdttools.a
WQ_LIB=${CCTOOLS_HOME}/work_queue/src/libwork_queue.a
VINE_LIB=${CCTOOLS_HOME}/taskvine/src/manager/libtaskvine.a

EXTERNAL_LIBRARIES=$(CHIRP_LIB) $(DT_LIB)

SOURCES = \
	batch_queue.c \
	batch_job.c \
	batch_job_info.c \
	batch_file.c \
	batch_wrapper.c \
	batch_queue_local.c \
	batch_queue_dryrun.c \
	batch_queue_condor.c \
	batch_queue_vine.c \
	batch_queue_work_queue.c \
	batch_queue_cluster.c \
	batch_queue_k8s.c \
	batch_queue_amazon.c

PUBLIC_HEADERS = batch_queue.h batch_job.h batch_job_info.h batch_file.h batch_wrapper.h

OBJECTS = $(SOURCES:%.c=%.o)

all: $(LIBRARIES) $(PROGRAMS) $(SCRIPTS)

libbatch_job.a: $(OBJECTS)

work_queue_factory: work_queue_factory.o libbatch_job.a $(WQ_LIB) $(VINE_LIB) $(EXTERNAL_LIBRARIES)
vine_factory: vine_factory.o libbatch_job.a $(WQ_LIB) $(VINE_LIB) $(EXTERNAL_LIBRARIES)

# Note that work_queue_pool is the same as work_queue_factory, for backwards compatibility.
work_queue_pool: work_queue_factory
	cp $< $@

condor_chaos_monkey:
	chmod 755 condor_chaos_monkey

install:
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(PUBLIC_HEADERS) $(CCTOOLS_INSTALL_DIR)/include/cctools

clean:
	rm -rf $(OBJECTS) $(LIBRARIES) $(PROGRAMS) *.o

