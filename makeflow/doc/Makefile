include ../../Makefile.config
include ../../Makefile.rules

all:

clean:

test:

man:
	rst2man makeflow.rst makeflow.1

install:
	install -d ${CCTOOLS_INSTALL_DIR}/doc
	install makeflow.html ${CCTOOLS_INSTALL_DIR}/doc
	install -d ${CCTOOLS_INSTALL_DIR}/share/man/man1
	install makeflow.1 ${CCTOOLS_INSTALL_DIR}/share/man/man1
	gzip ${CCTOOLS_INSTALL_DIR}/share/man/man1/makeflow.1
