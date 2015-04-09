include ../../config.mk
include ../../rules.mk

# We only use sqlite3 multi-process (i.e. no threads). So we can turn off
# SQLITE_THREADSAFE. This disables somewhat costly mutex locks.
LOCAL_CCFLAGS = -DSQLITE_THREADSAFE=0

# Globus is needed by all executables via libdttools.a/auth_globus.o
LOCAL_LINKAGE = $(CCTOOLS_GLOBUS_LDFLAGS)

EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
LIBRARIES = libchirp.a
OBJECTS = chirp_tool.o $(OBJECTS_LIBRARY) $(OBJECTS_SERVER)
OBJECTS_LIBRARY = $(SOURCES_LIBRARY:%.c=%.o)
OBJECTS_SERVER = $(SOURCES_SERVER:%.c=%.o)
PROGRAMS = chirp chirp_get chirp_put chirp_server chirp_status chirp_benchmark chirp_stream_files chirp_fuse chirp_distribute
PUBLIC_HEADERS = chirp_global.h chirp_multi.h chirp_reli.h chirp_client.h chirp_stream.h chirp_protocol.h chirp_matrix.h chirp_types.h chirp_recursive.h
SCRIPTS = chirp_audit_cluster chirp_server_hdfs
SOURCES_LIBRARY = chirp_global.c chirp_multi.c chirp_recursive.c chirp_reli.c chirp_client.c chirp_matrix.c chirp_stream.c chirp_ticket.c
SOURCES_SERVER = sqlite3.c chirp_stats.c chirp_thirdput.c chirp_alloc.c chirp_audit.c chirp_acl.c chirp_group.c chirp_filesystem.c chirp_fs_hdfs.c chirp_fs_local.c chirp_fs_local_scheduler.c chirp_fs_chirp.c chirp_job.c
TARGETS = $(PROGRAMS) $(LIBRARIES) bindings

all: $(TARGETS)

chirp: chirp_tool.o
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_READLINE_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE) $(CCTOOLS_READLINE_LDFLAGS)

chirp_fuse: chirp_fuse.o
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_FUSE_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)

chirp_fuse.o: chirp_fuse.c
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $(CCTOOLS_FUSE_CCFLAGS) $<

libchirp.a: $(OBJECTS_LIBRARY)

chirp_server: $(OBJECTS_SERVER)

$(PROGRAMS) $(CCTOOLS_SWIG_BINDINGS): libchirp.a $(EXTERNAL_DEPENDENCIES)

bindings: $(CCTOOLS_SWIG_BINDINGS)
$(CCTOOLS_SWIG_BINDINGS): chirp_swig_wrap.o
	@$(MAKE) -C $@

CCTOOLS_SWIG_BINDINGS_INSTALL = $(CCTOOLS_SWIG_BINDINGS:%=install-%)
install-bindings: $(CCTOOLS_SWIG_BINDINGS_INSTALL)
$(CCTOOLS_SWIG_BINDINGS_INSTALL): $(CCTOOLS_SWIG_BINDINGS)
	@$(MAKE) -C $(@:install-%=%) install

CCTOOLS_SWIG_BINDINGS_CLEAN = $(CCTOOLS_SWIG_BINDINGS:%=clean-%)
clean-bindings: $(CCTOOLS_SWIG_BINDINGS_CLEAN)
$(CCTOOLS_SWIG_BINDINGS_CLEAN):
	@$(MAKE) -C $(@:clean-%=%) clean

clean: clean-bindings
	rm -f $(OBJECTS) $(TARGETS)

install: all install-bindings
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	if [ -f $(CCTOOLS_INSTALL_DIR)/bin/chirp_server ]; then mv $(CCTOOLS_INSTALL_DIR)/bin/chirp_server $(CCTOOLS_INSTALL_DIR)/bin/chirp_server.old; fi
	chmod 755 $(SCRIPTS)
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(PUBLIC_HEADERS) $(CCTOOLS_INSTALL_DIR)/include/cctools/

test: all

.PHONY: all clean install test $(CCTOOLS_SWIG_BINDINGS) bindings $(CCTOOLS_SWIG_BINDINGS_INSTALL) install-bindings $(CCTOOLS_SWIG_BINDINGS_CLEAN) clean-bindings
