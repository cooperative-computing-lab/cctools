include ../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

TARGETS = work_queue.R work_queue.so

all: $(TARGETS)

work_queue.so: work_queue.R work_queue_wrap.c
	@PKG_CFLAGS="-I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/work_queue/src" R CMD SHLIB -o work_queue.so $(CCTOOLS_HOME)/work_queue/src/libwork_queue.a $(CCTOOLS_HOME)/dttools/src/libdttools.a work_queue_wrap.c

work_queue.R work_queue_wrap.c: work_queue.i
	swig -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/work_queue/src  -r work_queue.i

clean:
	-rm work_queue.R work_queue.so work_queue_wrap.c work_queue_wrap.o

install: 
	@echo Warning: r work_queue module needs to be installed manually.

