include ../../config.mk
include ../../rules.mk

all: prune.py prune_server prune_worker

prune.py: prune.in
	sed 's%CCTOOLS_VERSION%${CCTOOLS_VERSION}%' < prune.in | sed 's%CCTOOLS_RELEASE_DATE%${CCTOOLS_RELEASE_DATE}%' > prune.py
	chmod 755 prune.py

prune_server: prune_server.py
	sed 's%CCTOOLS_VERSION%${CCTOOLS_VERSION}%' < prune_server.py | sed 's%CCTOOLS_RELEASE_DATE%${CCTOOLS_RELEASE_DATE}%' > prune_server
	chmod 755 prune_server

prune_worker: prune_worker.py
	sed 's%CCTOOLS_VERSION%${CCTOOLS_VERSION}%' < prune_worker.py | sed 's%CCTOOLS_RELEASE_DATE%${CCTOOLS_RELEASE_DATE}%' > prune_worker
	chmod 755 prune_worker

clean:
	rm -f prune.py prune_server prune_worker

install: prune.py prune_worker prune_server
	cp prune.py ${CCTOOLS_INSTALL_DIR}/bin/prune
	cp prune_server ${CCTOOLS_INSTALL_DIR}/bin/prune_server
	cp prune_worker ${CCTOOLS_INSTALL_DIR}/bin/prune_worker
	mkdir -p $(CCTOOLS_PYTHON_PATH)/prune
	cp -r prune/* $(CCTOOLS_PYTHON_PATH)/prune
