include ../Makefile.config
include ../Makefile.rules

SCRIPTS=catalog_history_select catalog_history_filter catalog_history_plot

test:

all:

clean:

uninstall:
	for s in ${SCRIPTS}; do rm -f ${CCTOOLS_INSTALL_DIR}/bin/$$s; done

install:
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	for s in ${SCRIPTS}; do cat src/$$s | sed "s:CCTOOLS_VERSION:${CCTOOLS_VERSION}:g" | sed "s:CCTOOLS_RELEASEDATE:${CCTOOLS_RELEASEDATE}:g" > ${CCTOOLS_INSTALL_DIR}/bin/$$s; chmod 755 ${CCTOOLS_INSTALL_DIR}/bin/$$s; done


