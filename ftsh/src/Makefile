
include ../../Makefile.config
include ../../Makefile.rules

LOCAL_LDFLAGS=-ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

PROGRAMS=ftsh
LIBRARIES=libftsh.a
SOURCES=ftsh.c ftsh_error.c ast.c ast_print.c ast_execute.c expr.c parser.tab.c scanner.c variable.c buffer.c timed_exec.c multi_fork.c cancel.c builtin.o

OBJECTS = ${SOURCES:%.c=%.o}
HEADERS = ${SOURCES:%.c=%.h}

all: ${PROGRAMS} ${LIBRARIES}

ftsh: ftsh.o libftsh.a
	${CCTOOLS_LD} $^ -o $@ ${LOCAL_LDFLAGS}

libftsh.a: ${OBJECTS}
	ar rv $@ $^
	ranlib $@

parser.tab.c parser.tab.h: parser.y
	bison -d parser.y

scanner.c: scanner.l parser.tab.h
	flex -t scanner.l > scanner.c

test:

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	cp ftsh ${CCTOOLS_INSTALL_DIR}/bin

clean:
	rm -f ${PROGRAMS} ${OBJECTS} ${LIBRARIES} parser.tab.c parser.tab.h scanner.c *~
