CCTOOLS_HOME = ../..

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

SOURCES =   batch_job.c  \
	    batch_job_chirp.c  \
	    batch_job_condor.c  \
	    batch_job_hadoop.c  \
	    batch_job_local.c  \
	    batch_job_mpi_queue.c  \
	    batch_job_cluster.c  \
	    batch_job_work_queue.c \
	    work_queue_catalog.c   \
	    work_queue_resources.c \
	    work_queue_status.c    \
	    work_queue.c

PUBLIC_HEADERS = batch_job.h work_queue.h

LIBDTTOOLS = ${CCTOOLS_HOME}/dttools/src/libdttools.a
LIBCHIRP = $(CCTOOLS_HOME)/chirp/src/libchirp.a
EXTERNALS = $(LIBDTTOOLS) $(LIBCHIRP)

OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = ${SOURCES:%.c=%.h} ${EXTRA_HEADERS}
LIBRARIES = libwork_queue.a
PROGRAMS = work_queue_worker work_queue_status work_queue_example work_queue_pool

ifeq (${CCTOOLS_USE_MPI},true)
	PROGRAMS += mpi_queue_worker
endif

PROGRAM_SOURCES = ${PROGRAMS:%=%.c} ${TEST_PROGRAMS:%=%.c}

SCRIPTS = wq_submit_workers.common condor_submit_workers sge_submit_workers torque_submit_workers pbs_submit_workers ec2_submit_workers ec2_remove_workers slurm_submit_workers

TEST_PROGRAMS = work_queue_example work_queue_workload_simulator

all: ${LIBRARIES} $(PROGRAMS) $(TEST_PROGRAMS) bindings

work_queue_worker: work_queue_worker.o ${EXTERNALS} libwork_queue.a
	${CCTOOLS_LD} work_queue_worker.o ${CCTOOLS_INTERNAL_LDFLAGS} -lwork_queue -lchirp -ldttools -lm -o $@

mpi_queue_worker: mpi_queue_worker.o ${EXTERNALS} libwork_queue.a
	${CCTOOLS_MPI_LD} mpi_queue_worker.o ${CCTOOLS_INTERNAL_LDFLAGS} -lwork_queue -lchirp -ldttools -lm -o $@

work_queue_status: work_queue_status.o ${EXTERNALS} libwork_queue.a
	${CCTOOLS_LD} work_queue_status.o ${CCTOOLS_INTERNAL_LDFLAGS} -lwork_queue -lchirp -ldttools -lm -o $@

work_queue_example: work_queue_example.o ${EXTERNALS} libwork_queue.a
	${CCTOOLS_LD} work_queue_example.o ${CCTOOLS_INTERNAL_LDFLAGS} -lwork_queue -lchirp -ldttools -lm -o $@

work_queue_workload_simulator: work_queue_workload_simulator.o ${EXTERNALS} libwork_queue.a
	${CCTOOLS_LD} work_queue_workload_simulator.o ${CCTOOLS_INTERNAL_LDFLAGS} -lwork_queue -lchirp -ldttools -lm -o $@

work_queue_pool: work_queue_pool.o ${EXTERNALS} libwork_queue.a
	${CCTOOLS_LD} work_queue_pool.o ${CCTOOLS_INTERNAL_LDFLAGS} -lwork_queue -lchirp -ldttools -lm -ldl -o $@

libwork_queue.a: ${OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@


bindings: libwork_queue.a work_queue.i
	@for m in ${CCTOOLS_WORK_QUEUE_BINDINGS}; do cd $$m; $(MAKE) all; cd ..; done

clean-bindings:
	@for m in ${CCTOOLS_WORK_QUEUE_BINDINGS}; do cd $$m; $(MAKE) clean; cd ..; done

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	mkdir -p ${CCTOOLS_INSTALL_DIR}/lib
	mkdir -p ${CCTOOLS_INSTALL_DIR}/doc
	mkdir -p ${CCTOOLS_INSTALL_DIR}/include
	mkdir -p ${CCTOOLS_INSTALL_DIR}/include/cctools
	chmod 755 ${SCRIPTS}
	cp ${LIBRARIES} ${CCTOOLS_INSTALL_DIR}/lib
	cp ${PROGRAMS} ${CCTOOLS_INSTALL_DIR}/bin
	cp ${SCRIPTS} ${CCTOOLS_INSTALL_DIR}/bin
	cp work_queue_example.c ${CCTOOLS_INSTALL_DIR}/doc
	for file in ${PUBLIC_HEADERS} ; do if [ -f $$file ] ; then cp $$file ${CCTOOLS_INSTALL_DIR}/include/cctools ; fi ; done
	for file in ${CYGWINLIB} ; do if [ -f /bin/$$file ] ; then cp /bin/$$file ${CCTOOLS_INSTALL_DIR}/bin/$$file ; fi ; done

	@for m in ${CCTOOLS_WORK_QUEUE_BINDINGS}; do cd $$m; $(MAKE) $@; cd ..; done

test:

clean: clean-bindings
	rm -fr *.o ${LIBRARIES} ${PROGRAMS} ${TEST_PROGRAMS}

