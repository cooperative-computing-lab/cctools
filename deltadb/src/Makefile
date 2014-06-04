
include ../../Makefile.config
include ../../Makefile.rules

CCTOOLS_INTERNAL_LDFLAGS+=-ldttools -lm

SOURCES = deltadb_reduction.c
OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = ${SOURCES:%.c=%.h}
LIBRARIES = libdeltadb.a
PROGRAMS = deltadb_collect deltadb_select_static deltadb_select_dynamic deltadb_select_complete deltadb_project deltadb_reduce_temporal deltadb_reduce_spatial deltadb_pivot
PROGRAM_SOURCES = ${PROGRAMS:%=%.c}

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

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin

