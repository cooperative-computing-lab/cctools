include ../../Makefile.config
include ../../Makefile.rules

LOCAL_LDFLAGS = -ldttools -lchirp ${CCTOOLS_INTERNAL_LDFLAGS}

LIBRARY_SOURCES = chirp_global.c chirp_multi.c chirp_recursive.c chirp_reli.c chirp_client.c chirp_matrix.c chirp_stream.c chirp_ticket.c
SERVER_SOURCES = chirp_stats.c chirp_thirdput.c chirp_alloc.c chirp_audit.c chirp_acl.c chirp_group.c chirp_filesystem.c chirp_fs_hdfs.c chirp_fs_local.c chirp_fs_local_scheduler.c chirp_fs_chirp.c chirp_ticket.c chirp_job.c sqlite3.c

PROGRAMS=chirp chirp_get chirp_put chirp_server chirp_status chirp_benchmark chirp_stream_files chirp_fuse chirp_distribute
SCRIPTS=chirp_audit_cluster chirp_server_hdfs
LIBRARIES=libchirp.a libchirp_client.${CCTOOLS_DYNAMIC_SUFFIX}
PUBLIC_HEADERS=chirp_global.h chirp_multi.h chirp_reli.h chirp_client.h chirp_stream.h chirp_protocol.h chirp_matrix.h chirp_types.h chirp_recursive.h

TARGETS = ${PROGRAMS} ${LIBRARIES}
LIBRARY_OBJECTS = ${LIBRARY_SOURCES:%.c=%.o}
SERVER_OBJECTS = ${SERVER_SOURCES:%.c=%.o}

DTTOOLS_STATIC_LIBRARY = ../../dttools/src/libdttools.a
CHIRP_STATIC_LIBRARY = libchirp.a
STATIC_LIBRARIES = ${CHIRP_STATIC_LIBRARY} ${DTTOOLS_STATIC_LIBRARY}

all: ${TARGETS}

# This is the library intended to be used by clients of the system.

libchirp.a: ${LIBRARY_OBJECTS}
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

# This object exists primarily to get a linkable object for use
# by ROOT and other systems that want to load Chirp dynamically.
# Note that we explicitly include auth_all.o and ${CCTOOLS_DYNAMIC_AUTH_LIBS}
# to force the inclusion of the various authentication infrastructures.

libchirp_client.${CCTOOLS_DYNAMIC_SUFFIX}: ${LIBRARY_OBJECTS} ../../dttools/src/auth_all.o ${DTTOOLS_STATIC_LIBRARY}
	${CCTOOLS_LD} ${CCTOOLS_DYNAMIC_FLAG} -o $@ $^ ${CCTOOLS_DYNAMIC_AUTH_LIBS}

chirp: chirp_tool.o ${STATIC_LIBRARIES}
	${CCTOOLS_LD} -o $@ $^ ${CCTOOLS_READLINE_LDFLAGS} ${LOCAL_LDFLAGS}

chirp_server: chirp_server.o ${SERVER_OBJECTS} ${STATIC_LIBRARIES}
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_get: chirp_get.o ${STATIC_LIBRARIES}
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_put: chirp_put.o ${STATIC_LIBRARIES}
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_benchmark: chirp_benchmark.o ${STATIC_LIBRARIES}
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_status: chirp_status.o ${STATIC_LIBRARIES}
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_fuse: chirp_fuse.o ${STATIC_LIBRARIES}
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_distribute: chirp_distribute.o ${STATIC_LIBRARIES}
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_matrix_benchmark: chirp_matrix_benchmark.o ${STATIC_LIBRARIES}
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_matrix_roc: chirp_matrix_roc.o ${STATIC_LIBRARIES}
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_matrix_verify: chirp_matrix_verify.o ${STATIC_LIBRARIES}
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

chirp_stream_files: chirp_stream_files.o ${STATIC_LIBRARIES}
	${CCTOOLS_LD} -o $@ $^ ${LOCAL_LDFLAGS}

test: all

clean:
	rm -f core *~ *.o *.os *.a *.${CCTOOLS_DYNAMIC_SUFFIX} ${TARGETS}

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	mkdir -p ${CCTOOLS_INSTALL_DIR}/lib
	mkdir -p ${CCTOOLS_INSTALL_DIR}/include/cctools
	if [ -f ${CCTOOLS_INSTALL_DIR}/bin/chirp_server ]; then mv ${CCTOOLS_INSTALL_DIR}/bin/chirp_server ${CCTOOLS_INSTALL_DIR}/bin/chirp_server.old; fi
	chmod 755 ${SCRIPTS}
	cp ${PROGRAMS} ${SCRIPTS} ${CCTOOLS_INSTALL_DIR}/bin
	cp ${LIBRARIES} ${CCTOOLS_INSTALL_DIR}/lib
	cp ${PUBLIC_HEADERS} ${CCTOOLS_INSTALL_DIR}/include/cctools
