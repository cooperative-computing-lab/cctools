include ../../../config.mk
include ../../../rules.mk

CFLAGS := -I$(realpath ../../../dttools/src) -I$(realpath ../)
LDFLAGS := $(realpath ../libwork_queue.a) $(realpath ../../../dttools/src/libdttools.a) -lm -lz
TARGETS = work_queue.go

all: $(TARGETS)

work_queue.go: work_queue.i
	@echo "SWIG work_queue.i (golang)"
	@$(CCTOOLS_SWIG) -go -cgo -intgosize 64 -I../../../dttools/src -I../ work_queue.i 

work_queue_example: work_queue_example.go work_queue.go work_queue_wrap.c
	CGO_CFLAGS="$(CFLAGS)" CGO_LDFLAGS="$(LDFLAGS)" go build -o work_queue_example
test:

clean:
	rm work_queue.go work_queue_wrap.c 

install: 
