include ../../Makefile.config
include ../../Makefile.rules

SCRIPT = resource_monitor_visualizer.py
STATIC_RESOURCES = resource_monitor_visualizer_static

all: $(SCRIPT)

clean:

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	mkdir -p ${CCTOOLS_INSTALL_DIR}/lib
	cp ${SCRIPT} ${CCTOOLS_INSTALL_DIR}/bin/
	cp -r ${STATIC_RESOURCES} ${CCTOOLS_INSTALL_DIR}/lib/

uninstall:
	rm -rf ${CCTOOLS_INSTALL_DIR}/lib/$(STATIC_RESOURCES)
	rm -f ${CCTOOLS_INSTALL_DIR}/${SCRIPT}

.PHONY: all clean install test
