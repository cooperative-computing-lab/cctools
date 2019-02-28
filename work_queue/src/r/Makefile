include ../../../config.mk
include ../../../rules.mk

TARGETS = work_queue.R work_queue_wrap.so

all: $(TARGETS)

work_queue_wrap.so: work_queue.R work_queue_wrap.c
	@PKG_CFLAGS="-I$(realpath ../../../dttools/src) -I$(realpath ../)" PKG_LIBS="../libwork_queue.a" R CMD SHLIB work_queue_wrap.c

work_queue.R work_queue_wrap.c: work_queue.i
	swig -I../../../dttools/src -I../ -r work_queue.i

clean:
	-rm work_queue.R work_queue_wrap.so work_queue_wrap.c work_queue_wrap.o

install: 
	@echo Warning: r work_queue module needs to be installed manually.

