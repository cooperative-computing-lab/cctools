CCTOOLS_HOME = ../..

include ../../Makefile.config

all: bindings

bindings:
	@for m in ${CCTOOLS_WORK_QUEUE_BINDINGS}; do cd $$m; $(MAKE) all; cd ..; done

clean-bindings:
	@for m in ${CCTOOLS_WORK_QUEUE_BINDINGS}; do cd $$m; $(MAKE) clean; cd ..; done

install test: all
	@for m in ${CCTOOLS_WORK_QUEUE_BINDINGS}; do cd $$m; $(MAKE) $@; cd ..; done

clean: clean-bindings
	rm -fr *.o
