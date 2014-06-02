include ../Makefile.config
include ../Makefile.rules

SCRIPTS= deltadb_collect deltadb_select_static deltadb_select_dynamic deltadb_select_complete deltadb_project deltadb_reduce_temporal deltadb_reduce_spatial deltadb_pivot deltadb_test

all test install clean:
	cd src && $(MAKE) $@ && cd ..



uninstall:
	for s in ${SCRIPTS}; do rm -f ${CCTOOLS_INSTALL_DIR}/bin/$$s; done

install:
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	for s in ${SCRIPTS}; do cat src/$$s | sed "s:CCTOOLS_VERSION:${CCTOOLS_VERSION}:g" | sed "s:CCTOOLS_RELEASEDATE:${CCTOOLS_RELEASEDATE}:g" > ${CCTOOLS_INSTALL_DIR}/bin/$$s; chmod 755 ${CCTOOLS_INSTALL_DIR}/bin/$$s; done


