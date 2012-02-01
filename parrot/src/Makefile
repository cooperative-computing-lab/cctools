include ../../Makefile.config
include ../../Makefile.rules

TARGETS = parrot_run libparrot_client.a libparrot_helper.so parrot_lsalloc parrot_mkalloc parrot_getacl parrot_setacl parrot_whoami parrot_locate parrot_md5 parrot_cp parrot_timeout
PARROT_OBJECTS =  pfs_main.o pfs_poll.o tracer.o pfs_dispatch.o pfs_dispatch64.o pfs_process.o pfs_channel_cache.o pfs_channel.o pfs_sys.o pfs_table.o pfs_resolve.o pfs_service.o pfs_file.o pfs_file_cache.o pfs_dir.o pfs_dircache.o pfs_pointer.o pfs_location.o ibox_acl.o pfs_service_local.o pfs_service_http.o pfs_service_grow.o pfs_service_chirp.o pfs_service_multi.o pfs_service_nest.o pfs_service_ftp.o pfs_service_gfal.o pfs_service_lfc.o pfs_service_rfio.o pfs_service_dcap.o pfs_service_irods.o irods_reli.o pfs_service_hdfs.o pfs_service_bxgrid.o pfs_service_s3.o pfs_service_xrootd.o pfs_service_cvmfs.o

LOCAL_LDFLAGS=-lchirp -ls3client -ldttools -lftp_lite -ldl ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

parrot_run: ${PARROT_OBJECTS}
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

libparrot_helper.so: parrot_helper.c
	gcc -shared -fPIC $< -o $@

parrot_lsalloc: parrot_lsalloc.o parrot_client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_mkalloc: parrot_mkalloc.o parrot_client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_getacl: parrot_getacl.o parrot_client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_setacl: parrot_setacl.o parrot_client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_whoami: parrot_whoami.o parrot_client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_locate: parrot_locate.o parrot_client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_timeout: parrot_timeout.o parrot_client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_md5: parrot_md5.o parrot_client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_cp: parrot_cp.o parrot_client.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

libparrot_client.a: parrot_client.o
	ar rv $@ $^

pfs_main.o pfs_dispatch.o pfs_dispatch64.o pfs_process.o tracer.o: tracer.table.h tracer.table.c tracer.table64.h tracer.table64.c

tracer.table64.c: tracer.table64.in tracer.table.process
	perl tracer.table.process table 64 <$< >$@

tracer.table64.h: tracer.table64.in tracer.table.process
	perl tracer.table.process header 64 <$< >$@

tracer.table.c: tracer.table.in tracer.table.process
	perl tracer.table.process table 32 <$< >$@

tracer.table.h: tracer.table.in tracer.table.process
	perl tracer.table.process header 32 <$< >$@

test:

clean:
	rm -f core *~ *.o *.a ${TARGETS} tracer.table.c tracer.table.h tracer.table64.c tracer.table64.h

install: all parrot_client.h
	install -d ${CCTOOLS_INSTALL_DIR}
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install -d ${CCTOOLS_INSTALL_DIR}/lib
	install -d ${CCTOOLS_INSTALL_DIR}/include
	install -d ${CCTOOLS_INSTALL_DIR}/include/cctools
	install parrot_run ${CCTOOLS_INSTALL_DIR}/bin
	install libparrot_client.a ${CCTOOLS_INSTALL_DIR}/lib
	install parrot_client.h ${CCTOOLS_INSTALL_DIR}/include/cctools
	install libparrot_helper.so ${CCTOOLS_INSTALL_DIR}/lib
	install parrot_identity_box ${CCTOOLS_INSTALL_DIR}/bin
	install make_growfs ${CCTOOLS_INSTALL_DIR}/bin
	install parrot_mkalloc ${CCTOOLS_INSTALL_DIR}/bin
	install parrot_lsalloc ${CCTOOLS_INSTALL_DIR}/bin
	install parrot_setacl ${CCTOOLS_INSTALL_DIR}/bin
	install parrot_getacl ${CCTOOLS_INSTALL_DIR}/bin
	install parrot_whoami ${CCTOOLS_INSTALL_DIR}/bin
	install parrot_md5 ${CCTOOLS_INSTALL_DIR}/bin
	install parrot_cp ${CCTOOLS_INSTALL_DIR}/bin
	install parrot_locate ${CCTOOLS_INSTALL_DIR}/bin
	install parrot_timeout ${CCTOOLS_INSTALL_DIR}/bin
	chmod 755 parrot_run_hdfs
	install parrot_run_hdfs ${CCTOOLS_INSTALL_DIR}/bin
