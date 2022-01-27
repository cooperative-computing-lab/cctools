include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = ../../ftp_lite/src/libftp_lite.a ../../chirp/src/libchirp.a ../../grow/src/grow.o ../../dttools/src/libdttools.a
LIBRARIES = libparrot_helper.$(CCTOOLS_DYNAMIC_SUFFIX) libparrot_client.a
LOCAL_CXXFLAGS=$(CCTOOLS_IRODS_CCFLAGS) $(CCTOOLS_MYSQL_CCFLAGS) $(CCTOOLS_XROOTD_CCFLAGS) $(CCTOOLS_CVMFS_CCFLAGS) $(CCTOOLS_EXT2FS_CCFLAGS) $(CCTOOLS_GLOBUS_CCFLAGS) $(CCTOOLS_GLOBUS_CCFLAGS)
LOCAL_LDFLAGS=$(CCTOOLS_IRODS_LDFLAGS) $(CCTOOLS_MYSQL_LDFLAGS) $(CCTOOLS_XROOTD_LDFLAGS) $(CCTOOLS_CVMFS_LDFLAGS) $(CCTOOLS_EXT2FS_LDFLAGS) $(CCTOOLS_GLOBUS_LDFLAGS) $(CCTOOLS_GLOBUS_LDFLAGS)
OBJECTS = $(OBJECTS_PARROT_RUN) parrot_client.o pfs_resolve_mount.o
OBJECTS_PARROT_RUN = pfs_main.o tracer.o pfs_paranoia.o pfs_dispatch.o pfs_dispatch64.o pfs_process.o pfs_channel.o pfs_sys.o pfs_time.o pfs_table.o pfs_resolve.o pfs_mountfile.o pfs_service.o pfs_file.o pfs_file_cache.o pfs_dir.o pfs_dircache.o pfs_pointer.o pfs_location.o ibox_acl.o pfs_service_local.o pfs_service_http.o pfs_service_grow.o pfs_service_chirp.o pfs_service_multi.o pfs_service_nest.o pfs_service_ftp.o pfs_service_irods.o irods_reli.o pfs_service_hdfs.o pfs_service_bxgrid.o pfs_service_xrootd.o pfs_service_cvmfs.o pfs_service_ext.o
PROGRAMS = parrot_run $(UTILITIES)
HEADERS_PUBLIC = parrot_client.h
SCRIPTS = parrot_identity_box parrot_run_hdfs parrot_package_run chroot_package_run
TARGETS = $(PROGRAMS) $(LIBRARIES)
UTILITIES = parrot_lsalloc parrot_mkalloc parrot_getacl parrot_setacl parrot_whoami parrot_locate parrot_md5 parrot_cp parrot_timeout parrot_search parrot_package_create parrot_debug parrot_mount parrot_namespace

ifeq ($(CCTOOLS_BUILD_LIB64PARROT_HELPER),yes)
 LIBRARIES += lib64/libparrot_helper.$(CCTOOLS_DYNAMIC_SUFFIX)
endif

ifeq ($(CCTOOLS_BUILD_LIB32PARROT_HELPER),yes)
 LIBRARIES += lib/libparrot_helper.$(CCTOOLS_DYNAMIC_SUFFIX)
endif

all: $(TARGETS)

$(OBJECTS): tracer.table.h tracer.table.c tracer.table64.h tracer.table64.c

tracer.table.h tracer.table.c tracer.table64.h tracer.table64.c: tracer.table.pl syscall_parrot.tbl

libparrot_client.a: parrot_client.o

libparrot_helper.$(CCTOOLS_DYNAMIC_SUFFIX): parrot_helper.c

lib64/libparrot_helper.$(CCTOOLS_DYNAMIC_SUFFIX): parrot_helper.c
	mkdir -p lib64
	${CCTOOLS_CC} -m64 -shared -fPIC $< -o $@

lib/libparrot_helper.$(CCTOOLS_DYNAMIC_SUFFIX): parrot_helper.c
	mkdir -p lib
	${CCTOOLS_CC} -m32 -shared -fPIC $< -o $@

tracer.table64.c: syscall_64.tbl
	cat $< syscall_parrot.tbl | perl tracer.table.pl table 64 > $@

tracer.table64.h: syscall_64.tbl
	cat $< syscall_parrot.tbl | perl tracer.table.pl header 64 > $@

tracer.table.c: syscall_32.tbl
	cat $< syscall_parrot.tbl | perl tracer.table.pl table 32  > $@

tracer.table.h: syscall_32.tbl
	cat $< syscall_parrot.tbl | perl tracer.table.pl header 32 > $@

parrot_run: $(OBJECTS_PARROT_RUN) libparrot_client.a
	$(CCTOOLS_CXX) -o $@ $(CCTOOLS_INTERNAL_LDFLAGS) $^ $(CCTOOLS_IRODS_LDFLAGS) $(CCTOOLS_MYSQL_LDFLAGS) $(CCTOOLS_XROOTD_LDFLAGS) $(CCTOOLS_CVMFS_LDFLAGS) $(CCTOOLS_EXT2FS_LDFLAGS) $(CCTOOLS_GLOBUS_LDFLAGS) $(CCTOOLS_OPENSSL_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE_NO_OPENSSL)

parrot_cvmfs_static_run: $(OBJECTS_PARROT_RUN) libparrot_client.a $(CCTOOLS_CVMFS_LDFLAGS) $(EXTERNAL_DEPENDENCIES)
	$(CCTOOLS_CXX) -static -static-libstdc++ -static-libgcc -o $@ $^ $(CCTOOLS_INTERNAL_LDFLAGS) -Wl,-Bstatic $(CCTOOLS_OPENSSL_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE_NO_OPENSSL)

irods_reli.o: irods_reli.cc irods_reli.h
	$(CCTOOLS_CXX) -o $@ -c $(CCTOOLS_INTERNAL_CXXFLAGS) $< $(CCTOOLS_IRODS_CCFLAGS)

pfs_service_bxgrid.o: pfs_service_bxgrid.cc
	$(CCTOOLS_CXX) -o $@ -c $(CCTOOLS_INTERNAL_CXXFLAGS) $< $(CCTOOLS_MYSQL_CCFLAGS)

pfs_service_xrootd.o: pfs_service_xrootd.cc
	$(CCTOOLS_CXX) -o $@ -c $(CCTOOLS_INTERNAL_CXXFLAGS) $< $(CCTOOLS_XROOTD_CCFLAGS)

pfs_service_cvmfs.o: pfs_service_cvmfs.cc
	$(CCTOOLS_CXX) -o $@ -c $(CCTOOLS_INTERNAL_CXXFLAGS) $< $(CCTOOLS_CVMFS_CCFLAGS)

pfs_service_ext.o: pfs_service_ext.cc
	$(CCTOOLS_CXX) -o $@ -c $(CCTOOLS_INTERNAL_CXXFLAGS) $< $(CCTOOLS_EXT2FS_CCFLAGS)

pfs_service_ftp.o: pfs_service_ftp.cc
	$(CCTOOLS_CXX) -o $@ -c $(CCTOOLS_INTERNAL_CXXFLAGS) $< $(CCTOOLS_GLOBUS_CCFLAGS)

$(UTILITIES): libparrot_client.a
parrot_namespace: pfs_mountfile.o pfs_resolve_mount.o

$(PROGRAMS): $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(OBJECTS) $(TARGETS) $(PROGRAMS) $(LIBRARIES) tracer.table.c tracer.table.h tracer.table64.c tracer.table64.h *.o

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

test: all

.PHONY: all clean install test
