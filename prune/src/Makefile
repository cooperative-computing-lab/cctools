include ../../config.mk
include ../../rules.mk

all: prune.py

prune.py: prune.in
	sed 's%CCTOOLS_VERSION%${CCTOOLS_VERSION}%' < prune.in | sed 's%CCTOOLS_RELEASE_DATE%${CCTOOLS_RELEASE_DATE}%' > prune.py
	chmod 755 prune.py

clean:
	rm -f prune.py

install: prune.py
	cp prune.py ${CCTOOLS_INSTALL_DIR}/bin/prune
	mkdir -p $(CCTOOLS_PYTHON_PATH)/prune
	cp -r prune/* $(CCTOOLS_PYTHON_PATH)/prune
