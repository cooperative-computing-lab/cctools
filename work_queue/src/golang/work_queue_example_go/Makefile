include ../../../../config.mk
include ../../../../rules.mk

CFLAGS := -I$(realpath ../../../../dttools/src) -I$(realpath ../../)
LDFLAGS := $(realpath ../../libwork_queue.a) $(realpath ../../../../dttools/src/libdttools.a) -lm -lz
WQDIR := $(realpath ../)
TARGETS = work_queue_example

all: $(TARGETS)

work_queue_example: work_queue_example.go 
	echo $(CFLAGS)
	echo $(LDFLAGS)
	GOPATH="$(GOPATH):$(WQDIR)" CGO_CFLAGS="$(CFLAGS)" CGO_LDFLAGS="$(LDFLAGS)" go build
test:

clean:
	-rm work_queue_example

install: 
