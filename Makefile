include Makefile.config
include Makefile.rules

all clean: Makefile.config
	for p in ${CCTOOLS_PACKAGES} ; do cd $$p && ${MAKE} $@ && cd .. ; done

test: Makefile.config
	./run_all_tests.sh

install: all Makefile.config
	for p in ${CCTOOLS_PACKAGES} ; do cd $$p && ${MAKE} $@ && cd .. ; done
	mkdir -p ${CCTOOLS_INSTALL_DIR}/etc
	cp Makefile.config ${CCTOOLS_INSTALL_DIR}/etc/Makefile.config
	cp COPYING ${CCTOOLS_INSTALL_DIR}/doc

Makefile.config:
	@echo "You must run 'configure' before '${MAKE}'."
	@exit 1
