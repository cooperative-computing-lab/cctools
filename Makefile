include config.mk
include rules.mk

all: $(CCTOOLS_PACKAGES)

config.mk:
	@echo "You must run 'configure' before '${MAKE}'."
	@exit 1

allpairs chirp ftp_lite makeflow makeflow_linker parrot resource_monitor resource_monitor_visualizer sand wavefront work_queue : dttools

allpairs: sand work_queue
makeflow: chirp work_queue
parrot: chirp ftp_lite
sand: work_queue
wavefront: chirp work_queue
work_queue: chirp

$(CCTOOLS_PACKAGES): config.mk
	@$(MAKE) -C $@

CLEAN_PACKAGES = $(CCTOOLS_PACKAGES:%=clean-%)
$(CLEAN_PACKAGES):
	@$(MAKE) -C $(@:clean-%=%) clean
clean: $(CLEAN_PACKAGES)

INSTALL_PACKAGES = $(CCTOOLS_PACKAGES:%=install-%)
$(INSTALL_PACKAGES): $(CCTOOLS_PACKAGES)
	@$(MAKE) -C $(@:install-%=%) install
install: $(INSTALL_PACKAGES)
	mkdir -p ${CCTOOLS_INSTALL_DIR}/{etc,doc}
	cp config.mk ${CCTOOLS_INSTALL_DIR}/etc/config.mk
	cp COPYING ${CCTOOLS_INSTALL_DIR}/doc

test: $(CCTOOLS_PACKAGES)
	./run_all_tests.sh

.PHONY: $(CCTOOLS_PACKAGES) $(INSTALL_PACKAGES) $(CLEAN_PACKAGES) all clean install test
