
include ../../Makefile.config
include ../../Makefile.rules

CCTOOLS_INTERNAL_LDFLAGS+=-ldttools -lm

SOURCES = deltadb_reduction.c
OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = ${SOURCES:%.c=%.h}
LIBRARIES = libdeltadb.a
PROGRAMS = deltadb_collect deltadb_select_static deltadb_select_dynamic deltadb_select_complete deltadb_project deltadb_reduce_temporal deltadb_reduce_spatial deltadb_pivot
PROGRAM_SOURCES = ${PROGRAMS:%=%.c}
SCRIPTS= deltadb_collect deltadb_select_static deltadb_select_dynamic deltadb_select_complete deltadb_project deltadb_reduce_temporal deltadb_reduce_spatial deltadb_pivot

all: ${LIBRARIES} $(PROGRAMS)


deltadb_collect: deltadb_collect.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

deltadb_select_static: deltadb_select_static.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

deltadb_select_dynamic: deltadb_select_dynamic.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

deltadb_select_complete: deltadb_select_complete.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

deltadb_project: deltadb_project.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

deltadb_reduce_temporal: deltadb_reduce_temporal.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

deltadb_reduce_spatial: deltadb_reduce_spatial.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

deltadb_pivot: deltadb_pivot.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@


libdeltadb.a: ${OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

test: all

clean:
	rm -f core *~ *.o *.os *.so $(LIBRARIES) $(PROGRAMS)

uninstall:
	for s in ${SCRIPTS}; do rm -f ${CCTOOLS_INSTALL_DIR}/bin/$$s; done

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	for s in ${SCRIPTS}; do cat $$s | sed "s:CCTOOLS_VERSION:${CCTOOLS_VERSION}:g" | sed "s:CCTOOLS_RELEASEDATE:${CCTOOLS_RELEASEDATE}:g" > ${CCTOOLS_INSTALL_DIR}/bin/$$s; chmod 755 ${CCTOOLS_INSTALL_DIR}/bin/$$s; done

.PHONY: all clean install test uninstall
