include ../config.mk

all: ${CCTOOLS_DOCTARGETS}

htmlpages manpages mdpages:
	$(MAKE) -C man $@

apiperl: api/html/work_queue_perl.html api/html/work_queue_task_perl.html api/html/chirp_client_perl.html api/html/chirp_stat_perl.html

apipages: api/html/index.html apiperl

api/html/index.html:
	mkdir -p api/html
	doxygen cctools.doxygen.config

api/html/work_queue_perl.html:
	mkdir -p api/html
	pod2html ../work_queue/src/bindings/perl/Work_Queue.pm > $@
	@rm -f pod2htm*.tmp

api/html/work_queue_task_perl.html:
	mkdir -p api/html
	pod2html ../work_queue/src/bindings/perl/Work_Queue/Task.pm > $@
	@rm -f pod2htm*.tmp

api/html/chirp_client_perl.html:
	mkdir -p api/html
	pod2html ../chirp/src/bindings/perl/Chirp/Client.pm > $@
	@rm -f pod2htm*.tmp

api/html/chirp_stat_perl.html:
	mkdir -p api/html
	pod2html ../chirp/src/bindings/perl/Chirp/Stat.pm > $@
	@rm -f pod2htm*.tmp


install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	cp -r manuals $(CCTOOLS_INSTALL_DIR)/doc/
	if [ -d api ]; then cp -rp api $(CCTOOLS_INSTALL_DIR)/doc; fi
	$(MAKE) -C man install

test:

clean:
	rm -rf api *~
	$(MAKE) -C man clean

.PHONY: all clean install test
