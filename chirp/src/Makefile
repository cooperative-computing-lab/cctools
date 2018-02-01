include ../../config.mk
include ../../rules.mk

# -DSQLITE_DEFAULT_MMAP_SIZE=1073741824 - allow mmap of database.
# -DSQLITE_TEMP_STORE=3 - only use memory for temporary files.
# -DSQLITE_THREADSAFE=0 - we only use sqlite3 multi-process (i.e. no threads). This disables somewhat costly mutex locks.
LOCAL_CCFLAGS = -DSQLITE_DEFAULT_MMAP_SIZE=1073741824 -DSQLITE_TEMP_STORE=3 -DSQLITE_THREADSAFE=0 -fstack-protector-all

# Globus is needed by all executables via libdttools.a/auth_globus.o
LOCAL_LINKAGE = $(CCTOOLS_GLOBUS_LDFLAGS)

EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
LIBRARIES = libchirp.a libconfuga.a
OBJECTS = chirp_tool.o chirp_fuse.o $(OBJECTS_CONFUGA) $(OBJECTS_LIBRARY) $(OBJECTS_SERVER)
OBJECTS_CONFUGA = $(SOURCES_CONFUGA:%.c=%.o)
OBJECTS_LIBRARY = $(SOURCES_LIBRARY:%.c=%.o)
OBJECTS_SERVER = $(SOURCES_SERVER:%.c=%.o)
PROGRAMS = $(PROGRAMS_CHIRP) $(PROGRAMS_CONFUGA)
PROGRAMS_CHIRP = chirp chirp_get chirp_put chirp_server chirp_status chirp_benchmark chirp_stream_files chirp_fuse chirp_distribute
PROGRAMS_CONFUGA = confuga_adm
PUBLIC_HEADERS = chirp_global.h chirp_multi.h chirp_reli.h chirp_client.h chirp_stream.h chirp_protocol.h chirp_matrix.h chirp_types.h chirp_recursive.h confuga.h
SCRIPTS = chirp_audit_cluster chirp_server_hdfs
SOURCES_CONFUGA = confuga.c confuga_namespace.c confuga_replica.c confuga_node.c confuga_job.c confuga_file.c confuga_gc.c
SOURCES_LIBRARY = chirp_global.c chirp_multi.c chirp_recursive.c chirp_reli.c chirp_client.c chirp_matrix.c chirp_stream.c chirp_ticket.c
SOURCES_SERVER = sqlite3.c chirp_stats.c chirp_thirdput.c chirp_alloc.c chirp_audit.c chirp_acl.c chirp_group.c chirp_filesystem.c chirp_fs_hdfs.c chirp_fs_local.c chirp_fs_local_scheduler.c chirp_fs_chirp.c chirp_fs_confuga.c chirp_job.c chirp_sqlite.c
TARGETS = $(PROGRAMS) $(LIBRARIES) bindings

all: $(TARGETS)

chirp: chirp_tool.o
ifeq ($(CCTOOLS_STATIC),1)
	$(CCTOOLS_LD) -static -g -o $@ $(LOCAL_LINKAGE) $^ $(CCTOOLS_STATIC_LINKAGE)
else
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_READLINE_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE) $(CCTOOLS_READLINE_LDFLAGS)
endif

ifeq ($(CCTOOLS_STATIC),1)
chirp_fuse:
	@echo "chirp_fuse cannot be built statically"
else
chirp_fuse: chirp_fuse.o
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_FUSE_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)

chirp_fuse.o: chirp_fuse.c
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $(CCTOOLS_FUSE_CCFLAGS) $<
endif

chirp_job.o chirp_fs_local_scheduler.o: chirp_sqlite.h

# This is the library intended to be used by clients of the system.
libchirp.a: $(OBJECTS_LIBRARY)

confuga.o confuga_job.o confuga_namespace.o confuga_node.o confuga_replica.o confuga_file.o: confuga.h confuga_fs.h chirp_sqlite.h
libconfuga.a: $(OBJECTS_CONFUGA) $(OBJECTS_LIBRARY) chirp_sqlite.o sqlite3.o
libconfuga.$(CCTOOLS_DYNAMIC_SUFFIX): $(OBJECTS_CONFUGA) ../../dttools/src/auth_all.o $(EXTERNAL_DEPENDENCIES)

chirp_server: $(OBJECTS_SERVER) libconfuga.a
$(PROGRAMS_CONFUGA): libconfuga.a libchirp.a $(EXTERNAL_DEPENDENCIES)
$(PROGRAMS_CHIRP): libchirp.a $(EXTERNAL_DEPENDENCIES)
$(CCTOOLS_SWIG_BINDINGS): libchirp.a $(EXTERNAL_DEPENDENCIES)

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
	mkdir -p $(CCTOOLS_INSTALL_DIR)/share/cctools/chirp/
	cp -r ../tools/workflows/ $(CCTOOLS_INSTALL_DIR)/share/cctools/chirp/

test: all

.PHONY: all clean install test $(CCTOOLS_SWIG_BINDINGS) bindings $(CCTOOLS_SWIG_BINDINGS_INSTALL) install-bindings $(CCTOOLS_SWIG_BINDINGS_CLEAN) clean-bindings
