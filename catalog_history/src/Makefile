
include ../../Makefile.config
include ../../Makefile.rules

SOURCES = hash_cache.c  \
          hash_table.c  \
          nvpair.c  \
          nvpair_database.c  \
          debug.c  \
          xxmalloc.c  \
          stringtools.c  \
          timestamp.c  \
          full_io.c
          


PUBLIC_HEADERS = debug.h  \
                 int_sizes.h  \
                 timestamp.h

OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = ${SOURCES:%.c=%.h}
LIBRARIES = lib_catalog_history.a
PROGRAMS = ch_select ch_dfilter ch_sfilter ch_cfilter ch_project ch_treduce ch_pivot ch_sreduce

TEST_PROGRAMS = 
PROGRAM_SOURCES = ${PROGRAMS:%=%.c}
#CYGWINLIB = cygwin1.dll cyggcc_s-1.dll cygintl-8.dll cygreadline7.dll cygncursesw-10.dll cygiconv-2.dll cygattr-1.dll sh.exe

all: ${LIBRARIES} $(PROGRAMS) $(TEST_PROGRAMS)

ch_select: ch_select.o lib_catalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_dfilter: ch_dfilter.o lib_catalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_sfilter: ch_sfilter.o lib_catalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_cfilter: ch_cfilter.o lib_catalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_project: ch_project.o lib_catalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_treduce: ch_treduce.o lib_catalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_sreduce: ch_sreduce.o lib_catalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_pivot: ch_pivot.o lib_catalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@


lib_catalog_history.a: ${OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@


test: all

clean:
	rm -f core *~ *.o *.os *.so lib_catalog_history.a $(PROGRAMS) $(TEST_PROGRAMS)

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	mkdir -p ${CCTOOLS_INSTALL_DIR}/lib
	mkdir -p ${CCTOOLS_INSTALL_DIR}/doc
	mkdir -p ${CCTOOLS_INSTALL_DIR}/include
	mkdir -p ${CCTOOLS_INSTALL_DIR}/include/cctools
	cp ${LIBRARIES} ${CCTOOLS_INSTALL_DIR}/lib
	cp cctools_gpu_autodetect ${PROGRAMS} ${CCTOOLS_INSTALL_DIR}/bin
	for file in ${PUBLIC_HEADERS} ; do if [ -f $$file ] ; then cp $$file ${CCTOOLS_INSTALL_DIR}/include/cctools ; fi ; done
	for file in ${CYGWINLIB} ; do if [ -f /bin/$$file ] ; then cp /bin/$$file ${CCTOOLS_INSTALL_DIR}/bin/$$file ; fi ; done


