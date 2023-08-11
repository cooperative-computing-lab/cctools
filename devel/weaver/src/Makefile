include ../../../config.mk
include ../../../rules.mk

all: weaver.py

weaver.py: weaver.in
	sed 's%CCTOOLS_VERSION%${CCTOOLS_VERSION}%' < weaver.in > weaver.py
	chmod 755 weaver.py

install: weaver.py
	mkdir -p ${CCTOOLS_PYTHON_PATH}
	cp -r weaver ${CCTOOLS_PYTHON_PATH}
	cp weaver.py ${CCTOOLS_INSTALL_DIR}/bin/weaver

clean:
	rm -f weaver.py

test:
