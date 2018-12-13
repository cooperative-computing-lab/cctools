include ../../../config.mk
include ../../../rules.mk

CFLAGS := -I$(realpath ../../../dttools/src) -I$(realpath ../)
LDFLAGS := $(realpath ../libwork_queue.a) $(realpath ../../../dttools/src/libdttools.a) -lm -lz
TARGETS = chirp.go

all: $(TARGETS)

chirp.go: chirp.i
	@echo "SWIG chirp.i (golang)"
	@$(CCTOOLS_SWIG) -go -cgo -intgosize 64 -I../../../dttools/src -I../ chirp.i 

test:

clean:
	-rm chirp.go chirp_wrap.c

install: 
