include ../../config.mk
include ../../rules.mk

LIBRARY = libbatch_job.a

PROGRAMS = work_queue_pool

EXTERNAL_LIBRARIES = ../../work_queue/src/libwork_queue.a ../../dttools/src/libdttools.a ../../chirp/src/libchirp.a

SOURCES = \
	batch_job.c \
	batch_job_chirp.c \
	batch_job_cluster.c \
	batch_job_condor.c \
	batch_job_local.c \
	batch_job_work_queue.c

PUBLIC_HEADERS = batch_job.h

OBJECTS = $(SOURCES:%.c=%.o)

libbatch_job.a: $(OBJECTS)

work_queue_pool: libbatch_job.a work_queue_pool.o $(EXTERNAL_LIBRARIES)
	$(LD) $^ $(LDFLAGS) -o $@

all: $(LIBRARY) $(PROGRAMS)

clean:
	echo $(SOURCES) $(OBJECTS)
	rm -rf $(OBJECTS) $(LIBRARY) *~
