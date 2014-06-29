include ../../config.mk
include ../../rules.mk

LOCAL_CCFLAGS = -O4

EXTERNAL_DEPENDENCIES = ../../work_queue/src/libwork_queue.a ../../dttools/src/libdttools.a
LIBRARIES = libsandtools.a
OBJECTS = $(SOURCES:%.c=%.o)
PROGRAMS = sand_align_master sand_align_kernel sand_filter_master sand_filter_kernel sand_compress_reads sand_uncompress_reads
SCRIPTS = sand_runCA_5.4 sand_runCA_6.1 sand_runCA_7.0
SOURCES = compressed_sequence.o sequence_filter.c sequence.c matrix.c overlap.c align.c
TARGETS = $(PROGRAMS) $(LIBRARIES)

all: $(TARGETS)

libsandtools.a: $(OBJECTS)
$(PROGRAMS): $(LIBRARIES) $(EXTERNAL_DEPENDENCIES)

test: all

clean:
	rm -f $(OBJECTS) $(PROGRAMS) $(LIBRARIES)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	chmod 755 $(PROGRAMS) $(SCRIPTS)
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
