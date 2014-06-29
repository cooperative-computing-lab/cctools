include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
LIBRARIES = libftsh.a
OBJECTS = $(SOURCES:%.c=%.o)
PROGRAMS = ftsh
SOURCES = ast.c ast_execute.c ast_print.c buffer.c builtin.c cancel.c expr.c ftsh.c ftsh_error.c multi_fork.c parser.tab.c scanner.c timed_exec.c variable.c
TARGETS = $(PROGRAMS) $(LIBRARIES)

all: $(TARGETS)

libftsh.a: $(OBJECTS)
$(PROGRAMS): libftsh.a

parser.tab.c parser.tab.h: parser.y
	bison -d parser.y

scanner.c: scanner.l parser.tab.h
	flex -t scanner.l > scanner.c

test:

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(CCTOOLS_INSTALL_DIR)/bin/

clean:
	rm -f $(OBJECTS) $(TARGETS) parser.tab.c parser.tab.h scanner.c
