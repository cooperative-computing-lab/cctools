
include ../../Makefile.config
include ../../Makefile.rules

CCTOOLS_INTERNAL_LDFLAGS+=-ldttools -lm

SOURCES = nvpair.o reduction.o
OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = ${SOURCES:%.c=%.h}
LIBRARIES = libcatalog_history.a
PROGRAMS = ch_select ch_dfilter ch_sfilter ch_cfilter ch_project ch_treduce ch_pivot ch_sreduce
PROGRAM_SOURCES = ${PROGRAMS:%=%.c}

all: ${LIBRARIES} $(PROGRAMS)

ch_select: ch_select.o libcatalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_dfilter: ch_dfilter.o libcatalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_sfilter: ch_sfilter.o libcatalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_cfilter: ch_cfilter.o libcatalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_project: ch_project.o libcatalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_treduce: ch_treduce.o libcatalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_sreduce: ch_sreduce.o libcatalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@

ch_pivot: ch_pivot.o libcatalog_history.a
	${CCTOOLS_LD} $^ ${CCTOOLS_INTERNAL_LDFLAGS} -o $@


libcatalog_history.a: ${OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

test: all

clean:
	rm -f core *~ *.o *.os *.so $(LIBRARIES) $(PROGRAMS)

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin

