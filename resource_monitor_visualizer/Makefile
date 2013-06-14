include ../Makefile.config
include ../Makefile.rules

SCRIPT = bin/resource_monitor_visualizer

test:

all:

clean:

uninstall:
	rm -rf ${CCTOOLS_INSTALL_DIR}/lib/rmv ${CCTOOLS_INSTALL_DIR}/lib/rmv_static
	rm -f ${CCTOOLS_INSTALL_DIR}/${SCRIPT}

install: 
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	mkdir -p ${CCTOOLS_INSTALL_DIR}/lib
	chmod 755 ${SCRIPT}
	cp ${SCRIPT} ${CCTOOLS_INSTALL_DIR}/bin
	cp -r lib/rmv* ${CCTOOLS_INSTALL_DIR}/lib/

