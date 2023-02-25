include ../../../config.mk
include ../../../rules.mk

LOCAL_LINKAGE+=${CCTOOLS_HOME}/taskvine/src/manager/libtaskvine.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_CCFLAGS+=-I ${CCTOOLS_HOME}/taskvine/src/manager

PROGRAMS = vine_example_gutenberg vine_example_blast vine_example_mosaic vine_example_watch vine_example_minitask vine_example_poncho vine_example_peer_transfer
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
