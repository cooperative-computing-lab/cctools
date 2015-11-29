include ../../config.mk
include ../../rules.mk

# makeflow must link against globus:
# makeflow -> batch_job -> batch_job_chirp -> auth -> auth_globus -> globus
LOCAL_LINKAGE=$(CCTOOLS_GLOBUS_LDFLAGS)

EXTERNAL_DEPENDENCIES = ../../batch_job/src/libbatch_job.a ../../work_queue/src/libwork_queue.a ../../chirp/src/libchirp.a ../../dttools/src/libdttools.a
OBJECTS = dag.o dag_node.o dag_file.o dag_variable.o dag_task_category.o dag_visitors.o lexer.o parser.o
PROGRAMS = makeflow makeflow_viz makeflow_analyze makeflow_linker
SCRIPTS = condor_submit_makeflow makeflow_graph_log makeflow_monitor starch makeflow_linker_perl_driver makeflow_linker_python_driver
TARGETS = $(PROGRAMS)

all: $(TARGETS)

makeflow makeflow_viz makeflow_analyze: $(OBJECTS)

makeflow: makeflow_summary.o makeflow_gc.o makeflow_log.o makeflow_wrapper.o makeflow_wrapper_monitor.o makeflow_wrapper_docker.o

$(PROGRAMS): $(EXTERNAL_DEPENDENCIES)

lexer_test: dag.o dag_visitors.o makeflow_common.o lexer_test.o $(EXTERNAL_DEPENDENCIES)

lexer_test.o: lexer.c
	$(CCTOOLS_CC) -DLEXER_TEST -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $<

clean:
	rm -f *.o $(TARGETS) lexer_test*

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	chmod 755 $(SCRIPTS)
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/

test: all

.PHONY: all clean install test
