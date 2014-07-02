include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = ../../work_queue/src/libwork_queue.a ../../chirp/src/libchirp.a ../../dttools/src/libdttools.a
OBJECTS = dag.o visitors.o makeflow_common.o
PROGRAMS = makeflow makeflow_viz makeflow_analyze
SCRIPTS = condor_submit_makeflow makeflow_graph_log makeflow_monitor starch
TARGETS = $(PROGRAMS)

all: $(TARGETS)

$(PROGRAMS): $(OBJECTS) $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(OBJECTS) $(TARGETS)

test: all

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	chmod 755 $(SCRIPTS)
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
