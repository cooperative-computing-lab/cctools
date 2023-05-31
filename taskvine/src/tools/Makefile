include ../../../config.mk
include ../../../rules.mk

LOCAL_LINKAGE+=${CCTOOLS_HOME}/taskvine/src/manager/libtaskvine.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_CCFLAGS+=-I ${CCTOOLS_HOME}/taskvine/src/manager

PROGRAMS = vine_status vine_benchmark
SCRIPTS = vine_graph_log vine_graph_workers vine_plot_txn_log vine_profile_dispatch vine_condor_submit_workers vine_slurm_submit_workers vine_sge_submit_workers vine_pbs_submit_workers vine_torque_submit_workers vine_submit_common vine_submit_workers
TEST_PROGRAMS = vine_test
TARGETS = $(PROGRAMS) $(TEST_PROGRAMS)

all: $(TARGETS)

$(PROGRAMS) $(TEST_PROGRAMS): $(EXTERNALS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/

clean:
	rm -rf $(PROGRAMS) $(TEST_PROGRAMS) *.o

test: all

.PHONY: all clean install test
