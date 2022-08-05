include ../../../config.mk
include ../../../rules.mk

SCRIPTS = ds_submit_common condor_submit_workers sge_submit_workers torque_submit_workers pbs_submit_workers slurm_submit_workers ds_graph_log

all: $(SCRIPTS)

sge_submit_workers: sge_submit_workers_common
	sed -e "s/SGE_CUSTOM_PARAMETERS/${CCTOOLS_SGE_PARAMETERS}/" $< > $@
	chmod 755 $@

install: all
	chmod 755 $(SCRIPTS)
	cp $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/

clean:
	rm -rf sge_submit_workers

test: all

.PHONY: all clean install test bindings
