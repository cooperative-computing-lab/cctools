include ../../Makefile.config
include ../../Makefile.rules

PROGRAMS = sand_align_master sand_filter_master sand_banded_alignment sand_sw_alignment sand_filter_mer_seq sand_compress_reads sand_uncompress_reads
SOURCES =  sequence_compression.c sequence_filter.c sequence.c overlap.c align.c
OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = *.h

all: ${PROGRAMS}

${PROGRAMS}: %: %.o libsandtools.a
	${CCTOOLS_LD} -o $@ $< -L. -lsandtools -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

libsandtools.a: $(OBJECTS)
	ar rv libsandtools.a $(OBJECTS)
	ranlib libsandtools.a

%.o: %.c
	${CCTOOLS_CC} ${CCTOOLS_INTERNAL_CCFLAGS} -c $< -o $@ -O2

clean:
	rm -f core *~ *.o *.os *.so $(PROGRAMS) libsandtools.a

install: all
	install -d ${CCTOOLS_INSTALL_DIR}
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	for p in ${PROGRAMS}; do install $$p ${CCTOOLS_INSTALL_DIR}/bin; done
