include ../config.mk

all: ${CCTOOLS_DOCTARGETS}

htmlpages manpages mdpages:
	$(MAKE) -C man $@

apiperl: manuals/api/html/work_queue_perl.html manuals/api/html/work_queue_task_perl.html manuals/api/html/chirp_client_perl.html manuals/api/html/chirp_stat_perl.html

apipages: manuals/api/html/index.html

manuals/api/html/index.html:
	mkdir -p manuals/api/html
	doxygen api/cctools.doxygen.config

manuals/api/html/work_queue_perl.html:
	mkdir -p manuals/api/html
	pod2html ../work_queue/src/bindings/perl/Work_Queue.pm > $@
	@rm -f pod2htm*.tmp

manuals/api/html/work_queue_task_perl.html:
	mkdir -p manuals/api/html
	pod2html ../work_queue/src/bindings/perl/Work_Queue/Task.pm > $@
	@rm -f pod2htm*.tmp

manuals/api/html/chirp_client_perl.html:
	mkdir -p manuals/api/html
	pod2html ../chirp/src/bindings/perl/Chirp/Client.pm > $@
	@rm -f pod2htm*.tmp

manuals/api/html/chirp_stat_perl.html:
	mkdir -p manuals/api/html
	pod2html ../chirp/src/bindings/perl/Chirp/Stat.pm > $@
	@rm -f pod2htm*.tmp


install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc/cctools
	cp -a manuals $(CCTOOLS_INSTALL_DIR)/doc/cctools
	$(MAKE) -C man install

test:

clean:
	rm -rf manuals/api *~
	$(MAKE) -C man clean

.PHONY: all clean install test
