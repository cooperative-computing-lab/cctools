include config.mk
# export all variables in config.mk
export

include rules.mk


CYGWINLIB = cygwin1.dll cyggcc_s-1.dll cygintl-8.dll cygreadline7.dll cygncursesw-10.dll cygiconv-2.dll cygattr-1.dll sh.exe

#set the default values for RPM_VERSION and RPM_RELEASE
RPM_VERSION=4.4.1
RPM_RELEASE=1

all: $(CCTOOLS_PACKAGES)

config.mk:
	@echo "You must run 'configure' before '${MAKE}'."
	@exit 1

batch_job chirp dataswarm deltadb ftp_lite tlq makeflow makeflow_linker parrot resource_monitor work_queue grow: dttools
makeflow: batch_job
parrot: ftp_lite grow
batch_job parrot: chirp
batch_job makeflow: work_queue
batch_job makeflow: dataswarm

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
	cp README ${CCTOOLS_INSTALL_DIR}/doc/

test: $(CCTOOLS_PACKAGES)
	./run_all_tests.sh

rpm:
	./packaging/rpm/rpm_creator.sh $(RPM_VERSION) $(RPM_RELEASE)

.PHONY: $(CCTOOLS_PACKAGES) $(INSTALL_PACKAGES) $(CLEAN_PACKAGES) all clean install test rpm
