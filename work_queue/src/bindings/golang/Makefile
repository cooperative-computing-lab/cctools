include ../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

CFLAGS := -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/work_queue/src
LDFLAGS := $(realpath ../libwork_queue.a) $(CCTOOLS_HOME)/dttools/src/libdttools.a) -lm -lz
TARGETS = work_queue.go
EGDIR = work_queue_example_go

all: $(TARGETS)

work_queue.go: work_queue.i
	@echo "SWIG work_queue.i (golang)"
	@$(CCTOOLS_SWIG) -go -intgosize 64 -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/work_queue/src work_queue.i 
	@mkdir -p src/work_queue
	@mv work_queue.go work_queue_wrap.c src/work_queue

$(EGDIR): $(EGDIR)/work_queue_example.go src/work_queue/work_queue.go src/work_queue/work_queue_wrap.c
	$(MAKE) -C $@ 	

clean:
	-rm work_queue.go work_queue_wrap.c
	-rm -Rf src

install: 
	@echo Warning: go work_queue module needs to be installed manually.

