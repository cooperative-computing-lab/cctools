include ../../config.mk
include ../../rules.mk

# makeflow must link against globus:
# makeflow -> batch_job -> batch_job_chirp -> auth -> auth_globus -> globus
LOCAL_LINKAGE=$(CCTOOLS_GLOBUS_LDFLAGS)

EXTERNAL_DEPENDENCIES = ../../batch_job/src/libbatch_job.a ../../taskvine/src/manager/libtaskvine.a ../../work_queue/src/libwork_queue.a ../../chirp/src/libchirp.a ../../dttools/src/libdttools.a
OBJECTS = dag.o dag_node_footprint.o dag_node.o dag_file.o dag_variable.o dag_visitors.o dag_resources.o lexer.o parser.o parser_make.o parser_jx.o
PROGRAMS = makeflow makeflow_viz makeflow_analyze makeflow_linker makeflow_status
SCRIPTS = condor_submit_makeflow makeflow_graph_log makeflow_monitor starch makeflow_linker_perl_driver makeflow_linker_python_driver makeflow_archive_query mf_mesos_scheduler mf_mesos_executor mf_mesos_setting makeflow_ec2_setup makeflow_ec2_cleanup makeflow_ec2_estimate makeflow_lambda_setup makeflow_lambda_cleanup

SCRIPTS = condor_submit_makeflow sge_submit_makeflow makeflow_graph_log makeflow_monitor starch makeflow_linker_perl_driver makeflow_linker_python_driver makeflow_archive_query mf_mesos_scheduler mf_mesos_executor mf_mesos_setting makeflow_ec2_setup makeflow_ec2_cleanup makeflow_ec2_estimate makeflow_amazon_batch_setup makeflow_amazon_batch_cleanup makeflow_lambda_setup makeflow_lambda_cleanup

MAKEFLOW_WRAPPERS =  makeflow_mounts.o
MAKEFLOW_HOOKS = makeflow_hook_example.o
MAKEFLOW_MODULES = \
 makeflow_module_basic_wrapper.o\
 makeflow_module_docker.o\
 makeflow_module_enforcement.o\
 makeflow_module_fail_dir.o\
 makeflow_module_resource_monitor.o\
 makeflow_module_sandbox.o\
 makeflow_module_shared_fs.o\
 makeflow_module_singularity.o\
 makeflow_module_storage_allocation.o\
 makeflow_module_umbrella.o\
 makeflow_module_vc3_builder.o\

ifeq ($(CCTOOLS_CURL_AVAILABLE),yes)
CCTOOLS_EXTERNAL_LINKAGE += $(CCTOOLS_CURL_LDFLAGS) -lssl -lcrypto
MAKEFLOW_MODULES += makeflow_module_archive.o
endif


TARGETS = $(PROGRAMS)

all: $(TARGETS)

makeflow makeflow_viz makeflow_analyze makeflow_status: $(OBJECTS)

makeflow_status: makeflow_status.o

makeflow: makeflow_alloc.o makeflow_summary.o makeflow_gc.o makeflow_log.o makeflow_catalog_reporter.o makeflow_local_resources.o $(MAKEFLOW_WRAPPERS) makeflow_hook.o $(MAKEFLOW_HOOKS) $(MAKEFLOW_MODULES)


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
	mkdir -p $(CCTOOLS_INSTALL_DIR)/share/cctools/
	cp style.xml $(CCTOOLS_INSTALL_DIR)/share/cctools/makeflow-cytoscape-style.xml

test: all

.PHONY: all clean install test
