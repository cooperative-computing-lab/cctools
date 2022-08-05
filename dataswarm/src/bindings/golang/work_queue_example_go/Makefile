include ../../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

CFLAGS := -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/work_queue/src
LDFLAGS := $(CCTOOLS_HOME)/work_queue/src/libwork_queue.a $(CCTOOLS_HOME)/dttools/src/libdttools.a -lm -lz
TARGETS = ds_example

all: $(TARGETS)

ds_example: ds_example.go 
	echo $(CFLAGS)
	echo $(LDFLAGS)
	GOPATH="$(GOPATH):$(CCTOOLS_HOME)/work_queue/src" CGO_CFLAGS="$(CFLAGS)" CGO_LDFLAGS="$(LDFLAGS)" go build
test:

clean:
	-rm ds_example

install: 
