include Makefile.config
include Makefile.rules

all: $(CCTOOLS_PACKAGES)

Makefile.config:
	@echo "You must run 'configure' before '${MAKE}'."
	@exit 1

chirp work_queue sand allpairs wavefront makeflow ftp_lite parrot resource_monitor resource_monitor_visualizer makeflow_linker: dttools

allpairs: sand work_queue
makeflow: chirp work_queue
parrot: chirp ftp_lite
sand: work_queue
wavefront: chirp work_queue
work_queue: chirp

$(CCTOOLS_PACKAGES): Makefile.config
	@$(MAKE) -C $@

CLEAN_PACKAGES = $(CCTOOLS_PACKAGES:%=clean-%)
$(CLEAN_PACKAGES):
	@$(MAKE) -C $(@:clean-%=%) clean
clean: $(CLEAN_PACKAGES)

INSTALL_PACKAGES = $(CCTOOLS_PACKAGES:%=install-%)
$(INSTALL_PACKAGES): $(CCTOOLS_PACKAGES)
	@$(MAKE) -C $(@:install-%=%) install
install: $(INSTALL_PACKAGES)
	mkdir -p ${CCTOOLS_INSTALL_DIR}/etc
	cp Makefile.config ${CCTOOLS_INSTALL_DIR}/etc/Makefile.config
	cp COPYING ${CCTOOLS_INSTALL_DIR}/doc

test: $(CCTOOLS_PACKAGES)
	./run_all_tests.sh

.PHONY: $(CCTOOLS_PACKAGES) $(INSTALL_PACKAGES) $(CLEAN_PACKAGES) all clean install test
