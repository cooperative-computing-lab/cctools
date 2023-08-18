include ../../../config.mk
include ../../../rules.mk

LOCAL_LINKAGE+=${CCTOOLS_HOME}/taskvine/src/manager/libtaskvine.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_CCFLAGS+=-I ${CCTOOLS_HOME}/taskvine/src/manager

PROGRAMS = vine_example_gutenberg vine_example_blast vine_example_mosaic vine_example_watch vine_example_poncho
TARGETS = $(PROGRAMS)

all: $(TARGETS)

$(PROGRAMS): $(EXTERNALS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc/cctools/examples/taskvine/C
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc/cctools/examples/taskvine/python
	cp -r vine_example*.py script_example_for_poncho.py $(CCTOOLS_INSTALL_DIR)/doc/cctools/examples/taskvine/python
	for p in $(PROGRAMS); do cp $${p} $${p}.c $(CCTOOLS_INSTALL_DIR)/doc/cctools/examples/taskvine/C; done

clean:
	rm -rf $(PROGRAMS) *.o

test: all

lint:

format:

.PHONY: all clean install test lint format
