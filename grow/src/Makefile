include ../../config.mk
include ../../rules.mk

LOCAL_CCFLAGS = -D_FILE_OFFSET_BITS=64 -fstack-protector-all
LOCAL_LINKAGE = ../../dttools/src/libdttools.a

TARGETS =
OBJECTS = grow_fuse.o grow.o
SOURCES = grow_fuse.c grow.c
SCRIPTS = make_growfs

all: grow.o
ifeq ($(CCTOOLS_FUSE_AVAILABLE), yes)
all: grow_fuse
TARGETS += grow_fuse
endif

grow_fuse.c grow.c: grow.h

grow_fuse: grow_fuse.o grow.o
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_FUSE_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)

grow_fuse.o: grow_fuse.c
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $(CCTOOLS_FUSE_CCFLAGS) $<

clean:
	rm -f $(OBJECTS) $(TARGETS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	chmod 755 $(SCRIPTS)
	cp $(TARGETS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/

test: all

.PHONY: all clean install test
