include ../../config.mk
include ../../rules.mk

# Globus is needed by all executables via libdttools.a/auth_globus.o
LOCAL_LINKAGE = $(CCTOOLS_GLOBUS_LDFLAGS)

EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
LIBRARIES = libchirp.a
OBJECTS = chirp_tool.o chirp_fuse.o $(OBJECTS_LIBRARY) $(OBJECTS_SERVER) $(OBJECTS_PROGRAMS)
OBJECTS_LIBRARY = $(SOURCES_LIBRARY:%.c=%.o)
OBJECTS_SERVER = $(SOURCES_SERVER:%.c=%.o)
OBJECTS_PROGRAMS = $(PROGRAMS:%=%.o)
PROGRAMS = $(PROGRAMS_CHIRP)
PROGRAMS_CHIRP = chirp chirp_get chirp_put chirp_server chirp_status chirp_benchmark chirp_stream_files chirp_fuse chirp_distribute
PUBLIC_HEADERS = chirp_global.h chirp_multi.h chirp_reli.h chirp_client.h chirp_stream.h chirp_protocol.h chirp_matrix.h chirp_types.h chirp_recursive.h
SCRIPTS = chirp_audit_cluster chirp_server_hdfs
SOURCES_LIBRARY = chirp_global.c chirp_multi.c chirp_recursive.c chirp_reli.c chirp_client.c chirp_matrix.c chirp_stream.c chirp_ticket.c
SOURCES_SERVER = chirp_stats.c chirp_thirdput.c chirp_alloc.c chirp_audit.c chirp_acl.c chirp_group.c chirp_job.c chirp_filesystem.c chirp_fs_hdfs.c chirp_fs_local.c chirp_fs_chirp.c
TARGETS = $(PROGRAMS) $(LIBRARIES)

all: $(TARGETS) bindings

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

# This is the library intended to be used by clients of the system.
libchirp.a: $(OBJECTS_LIBRARY)

chirp_server: $(OBJECTS_SERVER)
$(PROGRAMS_CHIRP): libchirp.a $(EXTERNAL_DEPENDENCIES)

bindings: chirp_swig_wrap.o libchirp.a $(EXTERNAL_DEPENDENCIES)
	@$(MAKE) -C bindings

clean:
	rm -f $(OBJECTS) $(TARGETS)
	@$(MAKE) -C bindings clean

install: all
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
	@$(MAKE) -C bindings install

test: all
	@$(MAKE) -C bindings test

.PHONY: all clean install test bindings
