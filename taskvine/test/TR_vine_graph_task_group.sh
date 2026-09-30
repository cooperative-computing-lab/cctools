#!/bin/sh
set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH
export PATH=$(dirname "${CCTOOLS_PYTHON_TEST_EXEC}"):$PATH

CASE=chain-branches:6

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1
	"${CCTOOLS_PYTHON_TEST_EXEC}" -c "import cloudpickle" || return 1
	return 0
}

prepare()
{
	rm -f vine_graph_task_group_*.status vine_graph_task_group_*.port vine_graph_task_group_*.result vine_graph_task_group_*.log worker.*.log
	return 0
}

run_one()
{
	task_group=$1
	port_file=vine_graph_task_group_${task_group}.port
	status_file=vine_graph_task_group_${task_group}.status
	result_file=vine_graph_task_group_${task_group}.result
	run_log=vine_graph_task_group_${task_group}.log
	worker_log=worker.${task_group}.log

	rm -f $port_file $status_file $result_file $run_log $worker_log

	( ${CCTOOLS_PYTHON_TEST_EXEC} vine_graph_workflow_examples.py $port_file --case $CASE --task-group $task_group --result-file $result_file --no-print-results --timeout 90 > $run_log 2>&1; echo $? > $status_file ) &

	wait_for_file_creation $port_file 15

	cores=16
	memory=2000
	disk=2000
	run_taskvine_worker $port_file $worker_log

	wait_for_file_creation $status_file 60

	status=$(cat $status_file)
	if [ $status -ne 0 ]
	then
		exit 1
	fi

	test -s $result_file
}

print_throughput_comparison()
{
	without_group=$(awk '/^=== Throughput:/ {print $3 " " $4; exit}' vine_graph_task_group_0.log)
	with_group=$(awk '/^=== Throughput:/ {print $3 " " $4; exit}' vine_graph_task_group_1.log)

	echo "Task-group throughput comparison:"
	echo "  task-group=0: ${without_group:-unknown}"
	echo "  task-group=1: ${with_group:-unknown}"
}

run()
{
	run_one 0
	run_one 1
	print_throughput_comparison
	require_identical_files vine_graph_task_group_0.result vine_graph_task_group_1.result
	exit 0
}

clean()
{
	rm -f vine_graph_task_group_*.status vine_graph_task_group_*.port vine_graph_task_group_*.result vine_graph_task_group_*.log worker.*.log
	rm -rf vine-run-info
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
