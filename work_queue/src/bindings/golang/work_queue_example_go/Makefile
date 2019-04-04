include ../../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

CFLAGS := -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/work_queue/src
LDFLAGS := $(CCTOOLS_HOME)/work_queue/src/libwork_queue.a $(CCTOOLS_HOME)/dttools/src/libdttools.a -lm -lz
TARGETS = work_queue_example

all: $(TARGETS)

work_queue_example: work_queue_example.go 
	echo $(CFLAGS)
	echo $(LDFLAGS)
	GOPATH="$(GOPATH):$(CCTOOLS_HOME)/work_queue/src" CGO_CFLAGS="$(CFLAGS)" CGO_LDFLAGS="$(LDFLAGS)" go build
test:

clean:
	-rm work_queue_example

install: 
