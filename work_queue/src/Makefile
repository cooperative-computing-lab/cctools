CCTOOLS_HOME = ../..

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

PUBLIC_HEADERS = work_queue.h

LIBDTTOOLS = ${CCTOOLS_HOME}/dttools/src/libdttools.a

OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = ${SOURCES:%.c=%.h} ${EXTRA_HEADERS}
#LIBRARIES = libwork_queue.a
PROGRAMS = work_queue_worker

ifeq (${CCTOOLS_USE_MPI},true)
	PROGRAMS += mpi_queue_worker
endif

PROGRAM_SOURCES = ${PROGRAMS:%=%.c} ${TEST_PROGRAMS:%=%.c}

SCRIPTS = condor_submit_workers sge_submit_workers torque_submit_workers pbs_submit_workers ec2_submit_workers ec2_remove_workers

all: ${LIBRARIES} $(PROGRAMS) $(TEST_PROGRAMS) bindings

work_queue_worker: work_queue_worker.o ${LIBDTTOOLS}
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

mpi_queue_worker: mpi_queue_worker.o ${LIBDTTOOLS}
	${CCTOOLS_MPI_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

bindings:
	@for m in ${CCTOOLS_WORK_QUEUE_BINDINGS}; do cd $$m; $(MAKE) all; cd ..; done

clean-bindings:
	@for m in ${CCTOOLS_WORK_QUEUE_BINDINGS}; do cd $$m; $(MAKE) clean; cd ..; done

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	mkdir -p ${CCTOOLS_INSTALL_DIR}/lib
	mkdir -p ${CCTOOLS_INSTALL_DIR}/doc
	mkdir -p ${CCTOOLS_INSTALL_DIR}/include
	mkdir -p ${CCTOOLS_INSTALL_DIR}/include/cctools
	#chmod 755 ${SCRIPTS}
	#cp ${LIBRARIES} ${CCTOOLS_INSTALL_DIR}/lib
	cp ${PROGRAMS} ${CCTOOLS_INSTALL_DIR}/bin
	cp ${SCRIPTS} ${CCTOOLS_INSTALL_DIR}/bin
	cp work_queue_example.c ${CCTOOLS_INSTALL_DIR}/doc
	for file in ${PUBLIC_HEADERS} ; do if [ -f $$file ] ; then cp $$file ${CCTOOLS_INSTALL_DIR}/include/cctools ; fi ; done
	for file in ${CYGWINLIB} ; do if [ -f /bin/$$file ] ; then cp /bin/$$file ${CCTOOLS_INSTALL_DIR}/bin/$$file ; fi ; done

	@for m in ${CCTOOLS_WORK_QUEUE_BINDINGS}; do cd $$m; $(MAKE) $@; cd ..; done

test:

clean: clean-bindings
	rm -fr *.o

