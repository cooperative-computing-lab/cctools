include ../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

PERL_INSTALL_DIR = $(CCTOOLS_INSTALL_DIR)/lib/perl5/site_perl/$(CCTOOLS_PERL_VERSION)

# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
# Perl also defines some HAS_* stuff so we need to undefine those too.
LOCAL_CCFLAGS = -w -UHAS_USLEEP -UHAS_ISNAN -UHAS_UTIME -fPIC -Wno-unused-value -Wno-unused-variable $(CCTOOLS_PERL_CCFLAGS)
LOCAL_LINKAGE = $(CCTOOLS_PERL_LDFLAGS) -lz

EXTERNAL_DEPENDENCIES = $(CCTOOLS_HOME)/work_queue/src/libwork_queue.a $(CCTOOLS_HOME)/dttools/src/libdttools.a
WQPERLSO = work_queue.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = work_queue.pm $(WQPERLSO)
OBJECTS = work_queue_wrap.o
TARGETS = $(LIBRARIES)

all: $(TARGETS)

work_queue_wrap.c work_queue.pm: work_queue.i $(EXTERNAL_DEPENDENCIES)
	@echo "SWIG work_queue.i (perl5)"
	@$(CCTOOLS_SWIG) -o work_queue_wrap.c -perl5 -exportall -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/work_queue/src work_queue.i

$(WQPERLSO): $(OBJECTS) $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(OBJECTS) $(TARGETS) *.o work_queue_wrap.c

test: all

install: all
	mkdir -p $(PERL_INSTALL_DIR)
	cp $(LIBRARIES)       $(PERL_INSTALL_DIR)/
	mkdir -p              $(PERL_INSTALL_DIR)/Work_Queue
	cp Work_Queue.pm      $(PERL_INSTALL_DIR)
	cp Work_Queue/Task.pm $(PERL_INSTALL_DIR)/Work_Queue
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	chmod 755 work_queue_example.pl
	cp work_queue_example.pl $(CCTOOLS_INSTALL_DIR)/doc/
