include ../../../config.mk
include ../../../rules.mk

LOCAL_LINKAGE+=${CCTOOLS_HOME}/taskvine/src/manager/libtaskvine.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_CCFLAGS+=-I ${CCTOOLS_HOME}/taskvine/src/manager

PROGRAMS = ds_example_gutenberg ds_example_blast ds_example_mosaic ds_example_watch
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
