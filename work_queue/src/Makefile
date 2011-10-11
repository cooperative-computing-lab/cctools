CCTOOLS_HOME = ../..

include ../../Makefile.config

all: bindings

bindings:
	@for m in ${CCTOOLS_WORK_QUEUE_BINDINGS}; do cd $$m; make all; cd ..; done

clean-bindings:
	@for m in ${CCTOOLS_WORK_QUEUE_BINDINGS}; do cd $$m; make clean; cd ..; done

test: all
	@for m in ${CCTOOLS_WORK_QUEUE_BINDINGS}; do cd $$m; make test; cd ..; done

clean: clean-bindings
	rm -fr *.o

install: all
