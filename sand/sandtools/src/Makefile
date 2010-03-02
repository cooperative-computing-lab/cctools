CCTOOLS_HOME = ../../..
include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

SOURCES =  sequence_alignment.c sequence_compression.c sequence_filter.c ${CCTOOLS_HOME}/dttools/src/memory_info.c ${CCTOOLS_HOME}/dttools/src/itable.c

EXTRA_HEADERS = sand_align_macros.h 
OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = ${SOURCES:%.c=%.h} ${EXTRA_HEADERS}
LIBRARIES = libsandtools.a
CYGWINLIB = cygwin1.dll

all: ${LIBRARIES}

libsandtools.a: ${OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

${SOURCES} : ${CCTOOLS_HOME}/dttools/src/int_sizes.h 

${CCTOOLS_HOME}/dttools/src/int_sizes.h: ${CCTOOLS_HOME}/dttools/src/make_int_sizes
	./$< > $@

${CCTOOLS_HOME}/dttools/src/make_int_sizes: ${CCTOOLS_HOME}/dttools/src/make_int_sizes.o
	${CCTOOLS_LD} $^ -o $@ ${CCTOOLS_INTERNAL_LDFLAGS}


test:

clean:
	rm -f core *~ *.o *.os *.so libsandtools.a

install: all
	install -d ${CCTOOLS_INSTALL_DIR}
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install -d ${CCTOOLS_INSTALL_DIR}/lib
	install -d ${CCTOOLS_INSTALL_DIR}/include
	for file in ${LIBRARIES} ; do install $$file ${CCTOOLS_INSTALL_DIR}/lib/$$file ; done
	for file in ${HEADERS} ; do install $$file ${CCTOOLS_INSTALL_DIR}/include/$$(basename $$file) ; done
	if [ -f /bin/${CYGWINLIB} ] ; then install /bin/${CYGWINLIB} ${CCTOOLS_INSTALL_DIR}/bin/${CYGWINLIB} ; fi

