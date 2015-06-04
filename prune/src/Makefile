include ../../config.mk
include ../../rules.mk

PYTHON_INSTALL_DIR=$(CCTOOLS_INSTALL_DIR)/lib/python$(CCTOOLS_PYTHON_VERSION)/site-packages

all: prune.py

prune.py: prune.in
	m4 '-DCCTOOLS_VERSION=${CCTOOLS_VERSION}' < prune.in | m4 '-DCCTOOLS_RELEASE_DATE=${CCTOOLS_RELEASE_DATE}' > prune.py
	chmod 755 prune.py

clean:
	rm -f prune.py

install: prune.py
	cp prune.py ${CCTOOLS_INSTALL_DIR}/bin/prune
	mkdir -p ${PYTHON_INSTALL_DIR}/prune
	cp -r prune ${PYTHON_INSTALL_DIR}/prune
