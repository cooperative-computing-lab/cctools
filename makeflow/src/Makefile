include ../../config.mk
include ../../rules.mk

# makeflow must link against globus:
# makeflow -> batch_job -> batch_job_chirp -> auth -> auth_globus -> globus
LOCAL_LINKAGE=$(CCTOOLS_GLOBUS_LDFLAGS)

EXTERNAL_DEPENDENCIES = ../../work_queue/src/libwork_queue.a ../../chirp/src/libchirp.a ../../dttools/src/libdttools.a
OBJECTS = dag.o visitors.o makeflow_common.o lexer.o parser.o
PROGRAMS = makeflow makeflow_viz makeflow_analyze
SCRIPTS = condor_submit_makeflow makeflow_graph_log makeflow_monitor starch
TARGETS = $(PROGRAMS)

all: $(TARGETS)

$(PROGRAMS): $(OBJECTS) $(EXTERNAL_DEPENDENCIES)

lexer_test: dag.o visitors.o makeflow_common.o lexer_test.o $(EXTERNAL_DEPENDENCIES)

lexer_test.o: lexer.c
	$(CCTOOLS_CC) -DLEXER_TEST -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $<

clean:
	rm -f $(OBJECTS) $(TARGETS) lexer_test*

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	chmod 755 $(SCRIPTS)
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/

test: all

.PHONY: all clean install test
