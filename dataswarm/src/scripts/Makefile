include ../../../config.mk
include ../../../rules.mk

SCRIPTS = ds_graph_log ds_graph_workers

all: $(SCRIPTS)

install: all
	chmod 755 $(SCRIPTS)
	cp $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/

clean:

test: all

.PHONY: all clean install test bindings
