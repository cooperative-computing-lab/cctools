include config.mk
include rules.mk

CYGWINLIB = cygwin1.dll cyggcc_s-1.dll cygintl-8.dll cygreadline7.dll cygncursesw-10.dll cygiconv-2.dll cygattr-1.dll sh.exe

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
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	for file in $(CYGWINLIB) ; do if [ -f /bin/$$file ] ; then cp /bin/$$file $(CCTOOLS_INSTALL_DIR)/bin/ ; fi ; done
	mkdir -p ${CCTOOLS_INSTALL_DIR}/etc
	cp config.mk ${CCTOOLS_INSTALL_DIR}/etc/
	mkdir -p ${CCTOOLS_INSTALL_DIR}/doc
	cp COPYING ${CCTOOLS_INSTALL_DIR}/doc/

test: $(CCTOOLS_PACKAGES)
	./run_all_tests.sh

.PHONY: $(CCTOOLS_PACKAGES) $(INSTALL_PACKAGES) $(CLEAN_PACKAGES) all clean install test
