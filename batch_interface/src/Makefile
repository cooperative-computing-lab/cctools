CCTOOLS_HOME = ../..

include ../../Makefile.config
include ../../Makefile.rules

SOURCES =   batch_job.c  \
			batch_job_condor.c  \
			batch_job_hadoop.c  \
			batch_job_local.c  \
			batch_job_mpi_queue.c  \
			batch_job_cluster.c  \
			batch_job_work_queue.c

EXTRA_HEADERS = batch_job_internal.h

PUBLIC_HEADERS = batch_job.h

LIBDTTOOLS   = ${CCTOOLS_HOME}/dttools/src/libdttools.a
LIBWORKQUEUE = ${CCTOOLS_HOME}/work_queue/src/libwork_queue.a

OBJECTS   = ${SOURCES:%.c=%.o}
HEADERS   = ${SOURCES:%.c=%.h} ${EXTRA_HEADERS}
LIBRARIES = libdttools_batch.a
PROGRAMS  = work_queue_pool 

PROGRAM_SOURCES = ${PROGRAMS:%=%.c} ${TEST_PROGRAMS:%=%.c}

all: ${LIBRARIES} ${PROGRAMS}
	

work_queue_pool: work_queue_pool.o ${LIBDTTOOLS} ${LIBWORK_QUEUE} libdttools_batch.a
	${CCTOOLS_LD} work_queue_pool.o ${CCTOOLS_INTERNAL_LDFLAGS} -ldttools_batch -lwork_queue -ldttools -lm -o $@

libdttools_batch.a: ${OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

test:

clean:
	rm -f core *~ *.o *.os *.so libdttools.a large_chunk.txt $(PROGRAMS)

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	mkdir -p ${CCTOOLS_INSTALL_DIR}/lib
	mkdir -p ${CCTOOLS_INSTALL_DIR}/doc
	mkdir -p ${CCTOOLS_INSTALL_DIR}/include
	mkdir -p ${CCTOOLS_INSTALL_DIR}/include/cctools
	cp ${LIBRARIES} ${CCTOOLS_INSTALL_DIR}/lib
	cp ${PROGRAMS} ${CCTOOLS_INSTALL_DIR}/bin
	for file in ${PUBLIC_HEADERS} ; do if [ -f $$file ] ; then cp $$file ${CCTOOLS_INSTALL_DIR}/include/cctools ; fi ; done


