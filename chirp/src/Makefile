include ../../Makefile.config
include ../../Makefile.rules

LOCAL_LDFLAGS = -ldttools -lchirp ${CCTOOLS_INTERNAL_LDFLAGS}

LIBRARY_SOURCES = chirp_global.c chirp_multi.c chirp_recursive.c chirp_reli.c chirp_client.c chirp_matrix.c chirp_stream.c chirp_types.c chirp_acl.c chirp_local.c chirp_group.c chirp_hdfs.c chirp_filesystem.c
PROGRAMS=chirp chirp_get chirp_put chirp_server chirp_server_hdfs chirp_status chirp_benchmark chirp_stream_files chirp_fuse chirp_distribute chirp_md5sum
SCRIPTS=chirp_audit_cluster
LIBRARIES=libchirp.a
HEADERS=chirp_global.h chirp_multi.h chirp_reli.h chirp_client.h chirp_stream.h chirp_protocol.h chirp_matrix.h chirp_types.h

TARGETS = ${PROGRAMS} ${LIBRARIES}
OBJECTS = ${LIBRARY_SOURCES:%.c=%.o}

all: ${TARGETS}

libchirp.a: ${OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

chirp: chirp_tool.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_server: chirp_server.o chirp_stats.o chirp_job.o chirp_builtin.o chirp_thirdput.o chirp_alloc.o chirp_audit.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_server_hdfs: ../../hdfs-setup.template
	@cp $^ $@
	@chmod 755 $@
	@echo 'exec `dirname $$0`/chirp_server -f hdfs $$@' >> $@

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

chirp_distribute: chirp_distribute.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_matrix_benchmark: chirp_matrix_benchmark.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_matrix_roc: chirp_matrix_roc.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_matrix_verify: chirp_matrix_verify.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_md5sum: chirp_md5sum.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_stream_files: chirp_stream_files.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

test:	all

clean:
	rm -f core *~ *.o *.os *.a *.so ${TARGETS}

install: all
	install -d ${CCTOOLS_INSTALL_DIR}
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install -d ${CCTOOLS_INSTALL_DIR}/lib
	install -d ${CCTOOLS_INSTALL_DIR}/include
	install -d ${CCTOOLS_INSTALL_DIR}/include/cctools
	if [ -f ${CCTOOLS_INSTALL_DIR}/bin/chirp_server ]; then mv ${CCTOOLS_INSTALL_DIR}/bin/chirp_server ${CCTOOLS_INSTALL_DIR}/bin/chirp_server.old; fi
	for file in ${PROGRAMS} ${SCRIPTS} ; do install $$file ${CCTOOLS_INSTALL_DIR}/bin/$$file ; done
	for file in ${LIBRARIES} ; do install $$file ${CCTOOLS_INSTALL_DIR}/lib/$$file ; done
	for file in ${HEADERS} ; do install $$file ${CCTOOLS_INSTALL_DIR}/include/cctools/$$file ; done

