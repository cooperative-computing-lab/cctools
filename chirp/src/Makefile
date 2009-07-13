include ../../Makefile.config
include ../../Makefile.rules

TARGETS = chirp_server chirp_get chirp_put libchirp.a chirp chirp_benchmark chirp_status chirp_fuse chirp_distribute chirp_migrate chirp_matrix_benchmark chirp_md5sum chirp_partition_stream chirp_local_allpairs_multicore chirp_matrix_roc chirp_matrix_verify
LOCAL_LDFLAGS = -ldttools -lchirp ${CCTOOLS_INTERNAL_LDFLAGS}

LIBRARY_SOURCES = chirp_global.c chirp_multi.c chirp_recursive.c chirp_reli.c chirp_client.c chirp_matrix.c chirp_stream.c chirp_types.c chirp_acl.c chirp_local.c chirp_group.c

OBJECTS = ${LIBRARY_SOURCES:%.c=%.o}

all: ${TARGETS}

libchirp.a: ${OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

chirp: chirp_tool.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_server: chirp_server.o chirp_stats.o chirp_job.o chirp_builtin.o chirp_thirdput.o chirp_alloc.o chirp_audit.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_get: chirp_get.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_put: chirp_put.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_benchmark: chirp_benchmark.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_status: chirp_status.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_fuse: chirp_fuse.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_multi: chirp_multi_tool.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_distribute: chirp_distribute.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_migrate: chirp_migrate.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_matrix_benchmark: chirp_matrix_benchmark.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_matrix_roc: chirp_matrix_roc.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_matrix_verify: chirp_matrix_verify.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_md5sum: chirp_md5sum.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_local_allpairs_multicore: chirp_local_allpairs_multicore.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ -lpthread ${LOCAL_LDFLAGS}

chirp_partition_stream: chirp_partition_stream.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

test:

clean:
	rm -f core *~ *.o *.os *.a *.so ${TARGETS}

install: all
	install -d ${CCTOOLS_INSTALL_DIR}
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install -d ${CCTOOLS_INSTALL_DIR}/etc
	install -d ${CCTOOLS_INSTALL_DIR}/include
	install -d ${CCTOOLS_INSTALL_DIR}/lib
	install chirp ${CCTOOLS_INSTALL_DIR}/bin
	install chirp_get ${CCTOOLS_INSTALL_DIR}/bin
	install chirp_put ${CCTOOLS_INSTALL_DIR}/bin
	if [ -f ${CCTOOLS_INSTALL_DIR}/bin/chirp_server ]; then mv ${CCTOOLS_INSTALL_DIR}/bin/chirp_server ${CCTOOLS_INSTALL_DIR}/bin/chirp_server.old; fi
	install chirp_server ${CCTOOLS_INSTALL_DIR}/bin
	install chirp_status ${CCTOOLS_INSTALL_DIR}/bin
	install chirp_benchmark ${CCTOOLS_INSTALL_DIR}/bin
	install chirp_matrix_benchmark ${CCTOOLS_INSTALL_DIR}/bin
	install chirp_partition_stream ${CCTOOLS_INSTALL_DIR}/bin
	install chirp_fuse ${CCTOOLS_INSTALL_DIR}/bin
	install chirp_audit_cluster ${CCTOOLS_INSTALL_DIR}/bin
	install chirp_distribute ${CCTOOLS_INSTALL_DIR}/bin
	install chirp_migrate ${CCTOOLS_INSTALL_DIR}/bin
	install chirp_md5sum ${CCTOOLS_INSTALL_DIR}/bin
	install libchirp.a ${CCTOOLS_INSTALL_DIR}/lib
	install chirp_global.h ${CCTOOLS_INSTALL_DIR}/include
	install chirp_multi.h ${CCTOOLS_INSTALL_DIR}/include
	install chirp_reli.h ${CCTOOLS_INSTALL_DIR}/include
	install chirp_client.h ${CCTOOLS_INSTALL_DIR}/include
	install chirp_protocol.h ${CCTOOLS_INSTALL_DIR}/include
	install chirp_matrix.h ${CCTOOLS_INSTALL_DIR}/include
	install chirp_types.h ${CCTOOLS_INSTALL_DIR}/include
