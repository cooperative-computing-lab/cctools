include ../../config.mk
include ../../rules.mk

PHONY_TARGETS = wq_replica_exchange
SCRIPTS = makeflow_blast/makeflow_blast makeflow_bwa/makeflow_bwa makeflow_gatk/makeflow_gatk makeflow_blasr/makeflow_blasr wq_maker/wq_maker

all:

install:
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	chmod 755 $(SCRIPTS)
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/

doc:

clean:

test:
