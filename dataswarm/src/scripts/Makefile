include ../../../config.mk
include ../../../rules.mk

SCRIPTS = ds_submit_common ds_submit_condor_workers ds_submit_sge_workers ds_submit_torque_workers ds_submit_pbs_workers ds_submit_slurm_workers ds_graph_log ds_graph_workers

all: $(SCRIPTS)

ds_submit_sge_workers: ds_submit_sge_workers_common
	sed -e "s/SGE_CUSTOM_PARAMETERS/${CCTOOLS_SGE_PARAMETERS}/" $< > $@
	chmod 755 $@

install: all
	chmod 755 $(SCRIPTS)
	cp $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/

clean:
	rm -rf ds_submit_sge_workers

test: all

.PHONY: all clean install test bindings
