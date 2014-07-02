include ../../config.mk
include ../../rules.mk

# We only use sqlite3 multi-process (i.e. no threads). So we can turn off
# SQLITE_THREADSAFE. This disables somewhat costly mutex locks.
LOCAL_CCFLAGS = -DSQLITE_THREADSAFE=0

CHIRPSO = libchirp_client.$(CCTOOLS_DYNAMIC_SUFFIX)
EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
LIBRARIES = libchirp.a $(CHIRPSO)
OBJECTS = chirp_tool.o $(OBJECTS_LIBRARY) $(OBJECTS_SERVER)
OBJECTS_LIBRARY = $(SOURCES_LIBRARY:%.c=%.o)
OBJECTS_SERVER = $(SOURCES_SERVER:%.c=%.o)
PROGRAMS = chirp chirp_get chirp_put chirp_server chirp_status chirp_benchmark chirp_stream_files chirp_fuse chirp_distribute
PUBLIC_HEADERS = chirp_global.h chirp_multi.h chirp_reli.h chirp_client.h chirp_stream.h chirp_protocol.h chirp_matrix.h chirp_types.h chirp_recursive.h
SCRIPTS = chirp_audit_cluster chirp_server_hdfs
SOURCES_LIBRARY = chirp_global.c chirp_multi.c chirp_recursive.c chirp_reli.c chirp_client.c chirp_matrix.c chirp_stream.c chirp_ticket.c
SOURCES_SERVER = sqlite3.c chirp_stats.c chirp_thirdput.c chirp_alloc.c chirp_audit.c chirp_acl.c chirp_group.c chirp_filesystem.c chirp_fs_hdfs.c chirp_fs_local.c chirp_fs_local_scheduler.c chirp_fs_chirp.c chirp_job.c
TARGETS = $(PROGRAMS) $(LIBRARIES)

all: $(TARGETS)

# This object exists primarily to get a linkable object for use by ROOT and
# other systems that want to load Chirp dynamically.  Note that we explicitly
# include auth_all.o to force the inclusion of the various authentication
# infrastructures. We remove $(CCTOOLS_EXTERNAL_LINKAGE) because many archives
# we use were not compiled with -fPIC, which is required for shared object
# creation.
$(CHIRPSO): $(OBJECTS_LIBRARY) ../../dttools/src/auth_all.o $(EXTERNAL_DEPENDENCIES)
#	$(CCTOOLS_LD) -o $@ -fPIC $(CCTOOLS_DYNAMIC_FLAG) $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(CCTOOLS_DYNAMIC_AUTH_LIBS)

chirp: chirp_tool.o
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_EXTERNAL_LINKAGE) $(CCTOOLS_READLINE_LDFLAGS)

# This is the library intended to be used by clients of the system.
libchirp.a: $(OBJECTS_LIBRARY)

chirp_server: $(OBJECTS_SERVER)
$(PROGRAMS): libchirp.a $(EXTERNAL_DEPENDENCIES)

test: all

clean:
	rm -f $(OBJECTS) $(TARGETS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	if [ -f $(CCTOOLS_INSTALL_DIR)/bin/chirp_server ]; then mv $(CCTOOLS_INSTALL_DIR)/bin/chirp_server $(CCTOOLS_INSTALL_DIR)/bin/chirp_server.old; fi
	chmod 755 $(SCRIPTS)
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(PUBLIC_HEADERS) $(CCTOOLS_INSTALL_DIR)/include/cctools/
