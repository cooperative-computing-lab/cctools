include ../../Makefile.config
include ../../Makefile.rules

SOURCES = s3client.c
#s3put.c s3get.c s3stat.c s3rm.c s3mkdir.c s3rmdir.c s3ls.c s3getacl.c s3setacl.c
EXTRA_HEADERS = s3passwd.h
LOCAL_LDFLAGS = -lm -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = ${SOURCES:%.c=%.h} ${EXTRA_HEADERS}
LIBRARIES = libs3client.a
PROGRAMS = s3put s3get s3stat s3rm s3mkdir s3rmdir s3ls s3getacl s3setacl

PROGRAM_SOURCES = ${PROGRAMS:%=%.c}

OBJECTS = ${LIBRARY_SOURCES:%.c=%.o}

all: ${LIBRARIES} ${PROGRAMS}

s3put: s3put.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}
s3get: s3get.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}
s3stat: s3stat.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}
s3rm: s3rm.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}
s3mkdir: s3mkdir.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}
s3rmdir: s3rmdir.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}
s3ls: s3ls.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}
s3getacl: s3getacl.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}
s3setacl: s3setacl.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

s3passwd.h: mks3passwd.pl
	./mks3passwd.pl

libs3client.a: s3client.o
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

${SOURCES} ${PROGRAM_SOURCES} : s3passwd.h

install:

clean:
	rm -f core *~ *.o *.os *.so libs3client.a $(PROGRAMS)
