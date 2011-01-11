include ../../Makefile.config
include ../../Makefile.rules

PROGRAMS = sand_align_master sand_align_kernel sand_filter_master sand_filter_kernel sand_compress_reads sand_uncompress_reads
SCRIPTS = sand_runCA
SOURCES =  compressed_sequence.o sequence_filter.c sequence.c matrix.c overlap.c align.c
OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = *.h

all: ${PROGRAMS}

test: all

${PROGRAMS}: %: %.o libsandtools.a
	${CCTOOLS_LD} -o $@ $< -L. -lsandtools -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

libsandtools.a: $(OBJECTS)
	ar rv libsandtools.a $(OBJECTS)
	ranlib libsandtools.a

%.o: %.c
	${CCTOOLS_CC} ${CCTOOLS_INTERNAL_CCFLAGS} -c $< -o $@ -O4

clean:
	rm -f core *~ *.o *.os *.so $(PROGRAMS) libsandtools.a

install: all
	install -d ${CCTOOLS_INSTALL_DIR}
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	for p in ${PROGRAMS} ${SCRIPTS}; do install $$p ${CCTOOLS_INSTALL_DIR}/bin; done
