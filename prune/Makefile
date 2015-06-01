include ../config.mk
include ../rules.mk

PYTHON_INSTALL_DIR=$(CCTOOLS_INSTALL_DIR)/lib/python$(CCTOOLS_PYTHON_VERSION)/site-packages

all: prune.py

prune.py: prune.in
	sed 's/CCTOOLS_VERSION/${CCTOOLS_VERSION}/' < prune.in > prune.py
	chmod 755 prune.py

clean:
	rm prune.py

install: prune.py
	cp prune.py ${CCTOOLS_INSTALL_DIR}/bin/prune
	cp -r prune ${PYTHON_INSTALL_DIR}/prune
