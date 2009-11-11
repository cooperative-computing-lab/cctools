include Makefile.config
include Makefile.rules

all test clean: Makefile.config
	for p in ${CCTOOLS_PACKAGES} ; do cd $$p && ${MAKE} $@ && cd .. ; done

docs:
	${CCTOOLS_DOXYGEN} doxygen.config

install: all docs Makefile.config
	for p in ${CCTOOLS_PACKAGES} ; do cd $$p && ${MAKE} $@ && cd .. ; done
	if [ -d api ]; then cp -r api ${CCTOOLS_INSTALL_DIR}/doc; fi
	mkdir -p ${CCTOOLS_INSTALL_DIR}/etc
	cp Makefile.config ${CCTOOLS_INSTALL_DIR}/etc/Makefile.config
	cp COPYING ${CCTOOLS_INSTALL_DIR}/doc

Makefile.config:
	@echo "You must run 'configure' before '${MAKE}'."
	@exit 1
