include ../../../config.mk
include ../../../rules.mk

all: prune_worker #prune_server


prune_worker: prune_worker.py
	sed 's%CCTOOLS_VERSION%${CCTOOLS_VERSION}%' < prune_worker.py | sed 's%CCTOOLS_RELEASE_DATE%${CCTOOLS_RELEASE_DATE}%' > prune_worker
	chmod 755 prune_worker

clean:
	rm -f prune_worker #prune_server

install: prune_worker
	#cp prune_server ${CCTOOLS_INSTALL_DIR}/bin/prune_server
	mv prune_worker ${CCTOOLS_INSTALL_DIR}/bin/prune_worker
	mkdir -p $(CCTOOLS_PYTHON_PATH)/prune
	cp -r prune/* $(CCTOOLS_PYTHON_PATH)/prune
