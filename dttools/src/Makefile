
include ../../Makefile.config
include ../../Makefile.rules

SOURCES = auth.c  \
          auth_all.c  \
          auth_unix.c  \
          auth_globus.c  \
          auth_kerberos.c  \
          auth_hostname.c  \
          auth_address.c  \
          auth_ticket.c  \
          b64_encode.c  \
          bitmap.c  \
          buffer.c  \
          catalog_query.c  \
          cctools.c  \
          change_process_title.c  \
          chunk.c  \
          console_login.c  \
          clean_dir.c  \
          copy_stream.c  \
          create_dir.c  \
          daemon.c  \
          datagram.c  \
          debug.c  \
          delete_dir.c  \
          disk_info.c  \
          domain_name.c  \
          domain_name_cache.c  \
          dpopen.c  \
          envtools.c  \
          fast_popen.c  \
          full_io.c  \
          file_cache.c  \
          get_canonical_path.c  \
          get_line.c  \
          getopt.c  \
          getopt_aux.c  \
          hdfs_library.c  \
          hash_cache.c  \
          hash_table.c  \
          hmac.c  \
          http_query.c  \
          itable.c  \
          link.c  \
          link_auth.c  \
          link_nvpair.c \
          list.c  \
          load_average.c  \
          memory_info.c  \
          mergesort.c  \
          md5.c  \
          mpi_queue.c  \
          nvpair.c  \
          nvpair_database.c  \
          password_cache.c  \
          preadwrite.c  \
          process.c  \
          random_init.c \
          rmonitor.c \
          rmsummary.c \
          set.c \
          sha1.c  \
          sleeptools.c  \
          sort_dir.c  \
          stringtools.c  \
          string_array.c  \
          text_array.c  \
          text_list.c  \
          timestamp.c  \
          timer.c  \
          url_encode.c  \
          uptime.c \
          username.c  \
          xxmalloc.c

EXTRA_HEADERS = batch_job_internal.h \
                macros.h

PUBLIC_HEADERS = auth.h  \
                 auth_address.h  \
                 auth_all.h  \
                 auth_globus.h  \
                 auth_hostname.h  \
                 auth_kerberos.h  \
                 auth_ticket.h \
                 auth_unix.h  \
                 batch_job.h  \
                 buffer.h  \
                 debug.h  \
                 int_sizes.h  \
                 md5.h  \
                 rmonitor_hooks.h \
                 timestamp.h

OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = ${SOURCES:%.c=%.h} ${EXTRA_HEADERS}
LIBRARIES = libdttools.a
PROGRAMS = catalog_update catalog_server watchdog

TEST_PROGRAMS = microbench multirun
PROGRAM_SOURCES = ${PROGRAMS:%=%.c} ${TEST_PROGRAMS:%=%.c}
CYGWINLIB = cygwin1.dll cyggcc_s-1.dll cygintl-8.dll cygreadline7.dll cygncursesw-10.dll cygiconv-2.dll cygattr-1.dll sh.exe

all: ${LIBRARIES} $(PROGRAMS) $(TEST_PROGRAMS)

auth_test: auth_test.o libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

catalog_update: catalog_update.o libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

catalog_server: catalog_server.o libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

watchdog: watchdog.o libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

microbench: microbench.o libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

multirun: multirun.o libdttools.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

libdttools.a: ${OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

test: all

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
	for file in ${CYGWINLIB} ; do if [ -f /bin/$$file ] ; then cp /bin/$$file ${CCTOOLS_INSTALL_DIR}/bin/$$file ; fi ; done


