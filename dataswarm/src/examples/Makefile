include ../../../config.mk
include ../../../rules.mk

LOCAL_LINKAGE+=${CCTOOLS_HOME}/dataswarm/src/manager/libdataswarm.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_CCFLAGS+=-I ${CCTOOLS_HOME}/dataswarm/src/manager

PROGRAMS = ds_example ds_example_gutenberg ds_example_blast
TARGETS = $(PROGRAMS)

all: $(TARGETS)

$(PROGRAMS): $(EXTERNALS)

install: all
	# example programs are built but not installed here.
	# install to documentation instead?

clean:
	rm -rf $(PROGRAMS) *.o

test: all

.PHONY: all clean install test
