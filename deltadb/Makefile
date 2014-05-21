include ../Makefile.config
include ../Makefile.rules

SCRIPTS= ddb_collect ddb_select_static ddb_select_dynamic ddb_select_complete ddb_project ddb_reduce_temporal ddb_reduce_spatial ddb_pivot ddb_test

all test install clean:
	cd src && $(MAKE) $@ && cd ..



#uninstall:
#	for s in ${SCRIPTS}; do rm -f ${CCTOOLS_INSTALL_DIR}/bin/$$s; done

#install:
#	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
#	for s in ${SCRIPTS}; do cat src/$$s | sed "s:CCTOOLS_VERSION:${CCTOOLS_VERSION}:g" | sed "s:CCTOOLS_RELEASEDATE:${CCTOOLS_RELEASEDATE}:g" > ${CCTOOLS_INSTALL_DIR}/bin/$$s; chmod 755 ${CCTOOLS_INSTALL_DIR}/bin/$$s; done


