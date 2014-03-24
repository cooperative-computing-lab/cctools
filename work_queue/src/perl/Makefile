CCTOOLS_HOME = ../../..

include ${CCTOOLS_HOME}/Makefile.config

all: work_queue.pm work_queue.${CCTOOLS_DYNAMIC_SUFFIX}

work_queue_wrap.c work_queue.pm: work_queue.i
	@echo "SWIG work_queue.i (perl5)"
	@$(CCTOOLS_SWIG) -o work_queue_wrap.c -perl5 -exportall -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/work_queue/src work_queue.i

work_queue_wrap.o: work_queue_wrap.c
	$(CCTOOLS_CC) -c -o $@ -fPIC -Wno-unused-value -Wno-unused-variable -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/work_queue/src $(CCTOOLS_PERL_CCFLAGS) $^

work_queue.${CCTOOLS_DYNAMIC_SUFFIX}: work_queue_wrap.o $(CCTOOLS_HOME)/work_queue/src/libwork_queue.a $(CCTOOLS_HOME)/dttools/src/libdttools.a
	$(CCTOOLS_LD) $(CCTOOLS_DYNAMIC_FLAG) -o $@ $^ $(CCTOOLS_INTERNAL_LDFLAGS) $(CCTOOLS_PERL_LDFLAGS)

test: all

clean:
	rm -fr work_queue.pm work_queue.${CCTOOLS_DYNAMIC_SUFFIX} work_queue_wrap.*

PERL_INSTALL_DIR=$(CCTOOLS_INSTALL_DIR)/lib/perl5/site_perl/$(CCTOOLS_PERL_VERSION)

install: all
	mkdir -p $(PERL_INSTALL_DIR)
	mkdir -p ${CCTOOLS_INSTALL_DIR}/doc
	chmod 755 work_queue_example.pl
	cp work_queue.$(CCTOOLS_DYNAMIC_SUFFIX) $(PERL_INSTALL_DIR)
	cp work_queue.pm $(PERL_INSTALL_DIR)
	cp work_queue_example.pl ${CCTOOLS_INSTALL_DIR}/doc

