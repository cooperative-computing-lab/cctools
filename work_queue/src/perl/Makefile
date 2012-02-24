CCTOOLS_HOME = ../../..

include ${CCTOOLS_HOME}/Makefile.config

ccflags=${CCTOOLS_PERL_CCFLAGS} -I${CCTOOLS_HOME}/dttools/src
ldflags=${CCTOOLS_PERL_LDFLAGS} ${CCTOOLS_HOME}/dttools/src/libdttools.a

all: work_queue.pm work_queue.${CCTOOLS_DYNAMIC_SUFFIX}

work_queue_wrap.c work_queue.pm: ../work_queue.i
	@echo "SWIG work_queue.i (perl5)"
	@${CCTOOLS_SWIG} -perl5 -I${CCTOOLS_HOME}/dttools/src -o work_queue_wrap.c ../work_queue.i

work_queue_wrap.o: work_queue_wrap.c
	${CCTOOLS_CC} -fPIC -Wno-unused-value -Wno-unused-variable -c $< -o $@ ${ccflags}

work_queue.${CCTOOLS_DYNAMIC_SUFFIX}: work_queue_wrap.o
	${CCTOOLS_LD} ${CCTOOLS_DYNAMIC_FLAG} $< -o $@ ${ldflags}

test: all

clean:
	rm -fr work_queue.pm work_queue.${CCTOOLS_DYNAMIC_SUFFIX} work_queue_wrap.*

PERL_INSTALL_DIR=$(CCTOOLS_INSTALL_DIR)/lib/perl5/site_perl/$(CCTOOLS_PERL_VERSION)

install: all
	install -d $(PERL_INSTALL_DIR)
	install work_queue.$(CCTOOLS_DYNAMIC_SUFFIX) $(PERL_INSTALL_DIR)/work_queue.$(CCTOOLS_DYNAMIC_SUFFIX)
	install work_queue.pm $(PERL_INSTALL_DIR)/work_queue.pm
