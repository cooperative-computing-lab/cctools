include ../../Makefile.config
include ../../Makefile.rules

LOCAL_LDFLAGS = -ldttools -lchirp ${CCTOOLS_INTERNAL_LDFLAGS}

LIBRARY_SOURCES = chirp_global.c chirp_multi.c chirp_recursive.c chirp_reli.c chirp_client.c chirp_matrix.c chirp_stream.c chirp_ticket.c
SERVER_SOURCES = chirp_stats.c chirp_thirdput.c chirp_alloc.c chirp_audit.c chirp_acl.c chirp_group.c chirp_filesystem.c chirp_fs_hdfs.c chirp_fs_local.c chirp_fs_chirp.c chirp_ticket.c

PROGRAMS=chirp chirp_get chirp_put chirp_server chirp_status chirp_benchmark chirp_stream_files chirp_fuse chirp_distribute
SCRIPTS=chirp_audit_cluster chirp_server_hdfs
LIBRARIES=libchirp.a libchirp_client.${CCTOOLS_DYNAMIC_SUFFIX}
HEADERS=chirp_global.h chirp_multi.h chirp_reli.h chirp_client.h chirp_stream.h chirp_protocol.h chirp_matrix.h chirp_types.h chirp_recursive.h

TARGETS = ${PROGRAMS} ${LIBRARIES}
LIBRARY_OBJECTS = ${LIBRARY_SOURCES:%.c=%.o}
SERVER_OBJECTS = ${SERVER_SOURCES:%.c=%.o}

all: ${TARGETS}

# This is the library intended to be used by clients of the system.

libchirp.a: ${LIBRARY_OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

# This object exists primarily to get a linkable object for use
# by ROOT and other systems that want to load Chirp dynamically.
# Note that we explicitly include auth_all.o and ${CCTOOLS_DYNAMIC_AUTH_LIBS}
# to force the inclusion of the various authentication infrastructures.

libchirp_client.${CCTOOLS_DYNAMIC_SUFFIX}: ${LIBRARY_OBJECTS} ../../dttools/src/auth_all.o
	${CCTOOLS_LD} ${CCTOOLS_DYNAMIC_FLAG} -o $@ $^ ../../dttools/src/libdttools.a ${CCTOOLS_DYNAMIC_AUTH_LIBS}

chirp: chirp_tool.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${CCTOOLS_READLINE_LDFLAGS} ${LOCAL_LDFLAGS}

chirp_server: chirp_server.o ${SERVER_OBJECTS} libchirp.a
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

chirp_distribute: chirp_distribute.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_matrix_benchmark: chirp_matrix_benchmark.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_matrix_roc: chirp_matrix_roc.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_matrix_verify: chirp_matrix_verify.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_stream_files: chirp_stream_files.o libchirp.a
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

test:	all

clean:
	rm -f core *~ *.o *.os *.a *.${CCTOOLS_DYNAMIC_SUFFIX} ${TARGETS}

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

