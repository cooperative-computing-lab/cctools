include ../../config.mk
include ../../rules.mk

LOCAL_LDFLAGS = -fPIC
LOCAL_LINKAGE = -ldl

EXTERNAL_DEPENDENCIES = ../../ftp_lite/src/libftp_lite.a ../../chirp/src/libchirp.a ../../dttools/src/libdttools.a
LIBRARIES = libparrot_helper.$(CCTOOLS_DYNAMIC_SUFFIX) libparrot_client.a
OBJECTS = $(OBJECTS_PARROT_RUN) parrot_client.o
OBJECTS_PARROT_RUN = pfs_poll.o tracer.o pfs_paranoia.o pfs_dispatch.o pfs_dispatch64.o pfs_process.o pfs_channel.o pfs_sys.o pfs_table.o pfs_resolve.o pfs_service.o pfs_file.o pfs_file_cache.o pfs_dir.o pfs_dircache.o pfs_pointer.o pfs_location.o ibox_acl.o pfs_service_local.o pfs_service_http.o pfs_service_grow.o pfs_service_chirp.o pfs_service_multi.o pfs_service_nest.o pfs_service_ftp.o pfs_service_irods.o irods_reli.o pfs_service_hdfs.o pfs_service_bxgrid.o pfs_service_xrootd.o pfs_service_cvmfs.o
PROGRAMS = parrot_run $(UTILITIES)
HEADERS_PUBLIC = parrot_client.h
SCRIPTS = make_growfs parrot_identity_box parrot_run_hdfs parrot_package_run chroot_package_run
TARGETS = $(PROGRAMS) $(LIBRARIES)
UTILITIES = parrot_lsalloc parrot_mkalloc parrot_getacl parrot_setacl parrot_whoami parrot_locate parrot_md5 parrot_cp parrot_timeout parrot_search parrot_package_create

ifeq ($(CCTOOLS_BUILD_LIB64PARROT_HELPER),yes)
 LIBRARIES += lib64/libparrot_helper.$(CCTOOLS_DYNAMIC_SUFFIX)
endif

ifeq ($(CCTOOLS_BUILD_LIB32PARROT_HELPER),yes)
 LIBRARIES += lib/libparrot_helper.$(CCTOOLS_DYNAMIC_SUFFIX)
endif

all: $(TARGETS)

$(OBJECTS_PARROT_RUN): tracer.table.h tracer.table.c tracer.table64.h tracer.table64.c
libparrot_client.a: parrot_client.o

libparrot_helper.$(CCTOOLS_DYNAMIC_SUFFIX): parrot_helper.c

lib64/libparrot_helper.$(CCTOOLS_DYNAMIC_SUFFIX): parrot_helper.c
	mkdir -p lib64
	gcc -m64 -shared -fPIC $< -o $@

lib/libparrot_helper.$(CCTOOLS_DYNAMIC_SUFFIX): parrot_helper.c
	mkdir -p lib
	gcc -m32 -shared -fPIC $< -o $@

tracer.table64.c: tracer.table64.in tracer.table.process
	perl tracer.table.process table 64 <$< >$@

tracer.table64.h: tracer.table64.in tracer.table.process
	perl tracer.table.process header 64 <$< >$@

tracer.table.c: tracer.table.in tracer.table.process
	perl tracer.table.process table 32 <$< >$@

tracer.table.h: tracer.table.in tracer.table.process
	perl tracer.table.process header 32 <$< >$@

parrot_run: $(OBJECTS_PARROT_RUN)
$(UTILITIES): libparrot_client.a
$(PROGRAMS): $(EXTERNAL_DEPENDENCIES)

test:

clean:
	rm -f $(OBJECTS) $(TARGETS) $(PROGRAMS) $(LIBRARIES) tracer.table.c tracer.table.h tracer.table64.c tracer.table64.h

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	chmod 755 $(SCRIPTS)
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	cp $(HEADERS_PUBLIC) $(CCTOOLS_INSTALL_DIR)/include/cctools/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
ifeq ($(CCTOOLS_BUILD_LIB64PARROT_HELPER),yes)
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib/lib64
endif
ifeq ($(CCTOOLS_BUILD_LIB32PARROT_HELPER),yes)
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib/lib
endif
	for lib in $(LIBRARIES); do cp $$lib $(CCTOOLS_INSTALL_DIR)/lib/$$lib; done
