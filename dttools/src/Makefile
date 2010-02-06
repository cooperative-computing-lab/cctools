
include ../../Makefile.config
include ../../Makefile.rules

SOURCES = auth.c auth_all.c auth_unix.c auth_globus.c auth_kerberos.c auth_hostname.c auth_address.c batch_job.c bitmap.c catalog_query.c change_process_title.c console_login.c clean_dir.c copy_stream.c create_dir.c datagram.c debug.c delete_dir.c disk_info.c domain_name.c domain_name_cache.c fast_popen.c full_io.c file_cache.c get_canonical_path.c get_line.c hash_cache.c hash_table.c http_query.c itable.c link.c list.c load_average.c memory_info.c mergesort.c md5.c nvpair.c nvpair_database.c preadwrite.c process.c ragged_array.c sha1.c sleeptools.c sort_dir.c stringtools.c text_array.c timestamp.c timer.c url_encode.c username.c work_queue.c xmalloc.c

EXTRA_HEADERS = int_sizes.h macros.h

OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = ${SOURCES:%.c=%.h} ${EXTRA_HEADERS}
LIBRARIES = libdttools.a
PROGRAMS = catalog_server watchdog microbench multirun tar_stream worker worker_condor_submit work_queue_example
PROGRAM_SOURCES = ${PROGRAMS:%=%.c}
SCRIPTS = condor_submit_workers sge_submit_workers
CYGWINLIB = cygwin1.dll

all: ${LIBRARIES} $(PROGRAMS)

auth_test: auth_test.o libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

catalog_server: catalog_server.o libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

watchdog: watchdog.o libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

microbench: microbench.o libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

multirun: multirun.o libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

tar_stream: tar_stream.o libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

worker: worker.o libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

worker_condor_submit: worker_condor_submit.o
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

work_queue_example: work_queue_example.c libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

libdttools.a: ${OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

${SOURCES} ${PROGRAM_SOURCES} : int_sizes.h

int_sizes.h: make_int_sizes
	./$< > $@

make_int_sizes: make_int_sizes.o
	${CCTOOLS_LD} $^ -o $@ ${CCTOOLS_INTERNAL_LDFLAGS}

test:

clean:
	rm -f core *~ *.o *.os *.so libdttools.a make_int_sizes int_sizes.h $(PROGRAMS)

install: all
	install -d ${CCTOOLS_INSTALL_DIR}
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install -d ${CCTOOLS_INSTALL_DIR}/lib
	install -d ${CCTOOLS_INSTALL_DIR}/include
	for file in ${LIBRARIES} ; do install $$file ${CCTOOLS_INSTALL_DIR}/lib/$$file ; done
	for file in ${HEADERS} ; do install $$file ${CCTOOLS_INSTALL_DIR}/include/$$file ; done
	for file in ${PROGRAMS} ; do install $$file ${CCTOOLS_INSTALL_DIR}/bin/$$file ; done
	for file in ${SCRIPTS} ; do install $$file ${CCTOOLS_INSTALL_DIR}/bin/$$file ; chmod 755 ${CCTOOLS_INSTALL_DIR}/bin/$$file; done
	if [ -f /bin/${CYGWINLIB} ] ; then install /bin/${CYGWINLIB} ${CCTOOLS_INSTALL_DIR}/bin/${CYGWINLIB} ; fi

