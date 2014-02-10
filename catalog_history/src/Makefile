
include ../../Makefile.config
include ../../Makefile.rules

CCTOOLS_INTERNAL_LDFLAGS+=-ldttools -lm

SOURCES = nvpair.o reduction.o deltadb_value.o deltadb_expr.o deltadb_scanner.o deltadb_parser.o
OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = ${SOURCES:%.c=%.h}
LIBRARIES = libdeltadb.a
PROGRAMS = ch_select ch_dfilter ch_sfilter ch_cfilter ch_project ch_treduce ch_pivot ch_sreduce deltadb_test
PROGRAM_SOURCES = ${PROGRAMS:%=%.c}

all: ${LIBRARIES} $(PROGRAMS)

deltadb_test: deltadb_test.c libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_select: ch_select.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_dfilter: ch_dfilter.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_sfilter: ch_sfilter.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_cfilter: ch_cfilter.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_project: ch_project.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_treduce: ch_treduce.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_sreduce: ch_sreduce.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_pivot: ch_pivot.o libdeltadb.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@


libdeltadb.a: ${OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

test: all

clean:
	rm -f core *~ *.o *.os *.so $(LIBRARIES) $(PROGRAMS)

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin

