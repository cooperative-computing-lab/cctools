include ../../Makefile.config
include ../../Makefile.rules

all: ftp_lite_test ftp_lite_copy libftp_lite.a

ftp_lite_test: ftp_lite_test.o libftp_lite.a
	${CCTOOLS_LD} $^ -o $@ -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

ftp_lite_copy: ftp_lite_copy.o libftp_lite.a
	${CCTOOLS_LD} $^ -o $@ -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

libftp_lite.a: ftp_lite.o error.o radix.o login.o stream.o network.o
	${CCTOOLS_AR} rv $@ $^
	ranlib $@

test:

clean:
	rm -f ftp_lite_test ftp_lite_copy libftp_lite.a core *~ *.o *.so so_locations

install:
	${CCTOOLS_INSTALL} -d ${CCTOOLS_INSTALL_DIR}/bin
	${CCTOOLS_INSTALL} -d ${CCTOOLS_INSTALL_DIR}/lib
	${CCTOOLS_INSTALL} -d ${CCTOOLS_INSTALL_DIR}/include
	${CCTOOLS_INSTALL} libftp_lite.a ${CCTOOLS_INSTALL_DIR}/lib
	${CCTOOLS_INSTALL} ftp_lite.h ${CCTOOLS_INSTALL_DIR}/include/cctools
