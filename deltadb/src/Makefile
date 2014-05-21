
include ../../Makefile.config
include ../../Makefile.rules

CCTOOLS_INTERNAL_LDFLAGS+=-ldttools -lm

SOURCES = nvpair.c deltadb_reduction.c deltadb_value.c deltadb_expr.c deltadb_scanner.c deltadb_parser.c deltadb_functions.c
OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = ${SOURCES:%.c=%.h}
LIBRARIES = libdeltadb.a
PROGRAMS = ddb_collect ddb_select_static ddb_select_dynamic ddb_select_complete ddb_project ddb_reduce_temporal ddb_reduce_spatial ddb_pivot ddb_test
PROGRAM_SOURCES = ${PROGRAMS:%=%.c}

all: ${LIBRARIES} $(PROGRAMS)

ddb_test: ddb_test.c libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ddb_collect: ddb_collect.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ddb_select_static: ddb_select_static.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ddb_select_dynamic: ddb_select_dynamic.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ddb_select_complete: ddb_select_complete.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ddb_project: ddb_project.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ddb_reduce_temporal: ddb_reduce_temporal.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ddb_reduce_spatial: ddb_reduce_spatial.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ddb_pivot: ddb_pivot.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@


libdeltadb.a: ${OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

test: all

clean:
	rm -f core *~ *.o *.os *.so $(LIBRARIES) $(PROGRAMS)

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin

