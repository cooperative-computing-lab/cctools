include ../../Makefile.config
include ../../Makefile.rules

TARGETS = parrot libparrot.a parrot_helper.so parrot_lsalloc parrot_mkalloc parrot_getacl parrot_setacl parrot_whoami parrot_locate parrot_md5 parrot_cp

LOCAL_LDFLAGS=-lchirp -ldttools -lftp_lite -ldl ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

parrot: pfs_main.o pfs_poll.o tracer.o pfs_dispatch.o pfs_dispatch64.o pfs_process.o pfs_channel_cache.o pfs_channel.o libparrot.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_helper.so: parrot_helper.c
	gcc -shared -Wl,-rpath=/lib64 -fPIC $< -o $@

parrot_lsalloc: parrot_lsalloc.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_mkalloc: parrot_mkalloc.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_getacl: parrot_getacl.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_setacl: parrot_setacl.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_whoami: parrot_whoami.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_locate: parrot_locate.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_md5: parrot_md5.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

parrot_cp: parrot_cp.o
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

libparrot.a: pfs_sys.o pfs_table.o pfs_resolve.o pfs_service.o pfs_file.o pfs_file_cache.o pfs_dir.o pfs_dircache.o pfs_pointer.o pfs_location.o pfs_service_local.o pfs_service_http.o pfs_service_grow.o pfs_service_chirp.o pfs_service_multi.o pfs_service_nest.o pfs_service_ftp.o pfs_service_gfal.o pfs_service_lfc.o pfs_service_rfio.o pfs_service_dcap.o pfs_file_gzip.o pfs_service_irods.o irods_reli.o ${PFS_OPTIONAL_SERVICES}
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

install: all
	install -d ${CCTOOLS_INSTALL_DIR}
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install -d ${CCTOOLS_INSTALL_DIR}/etc
	install parrot ${CCTOOLS_INSTALL_DIR}/bin
	install parrot_helper.so ${CCTOOLS_INSTALL_DIR}/bin
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
	test -x parrot_hdfs && install parrot_hdfs ${CCTOOLS_INSTALL_DIR}/bin || true
