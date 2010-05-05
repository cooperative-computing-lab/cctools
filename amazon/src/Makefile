include ../../Makefile.config
include ../../Makefile.rules

LIBRARY_SOURCES = s3client.c

OBJECTS = ${LIBRARY_SOURCES:%.c=%.o}

all: libs3client.a

test: test.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm -ldttools
s3put: s3put.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm -ldttools
s3get: s3get.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm -ldttools
s3stat: s3stat.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm -ldttools
s3rm: s3rm.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm -ldttools
s3mkdir: s3mkdir.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm -ldttools
s3rmdir: s3rmdir.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm -ldttools
s3ls: s3ls.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm -ldttools
s3getacl: s3getacl.o s3client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS} -lm -ldttools

libs3client.a: s3client.o
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

install:

clean:
	rm -f *.o
