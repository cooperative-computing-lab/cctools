#!/bin/sh

# For each month of the year, list each project run in that month,
# with the owner and number of tasks dispatched.



DIR=${CCTOOLS}/bin
DATA=/var/tmp/catalog.history


${DIR}/deltadb_collect ${DATA} 2013-02-1@00:00:00 d365 | \
${DIR}/deltadb_select_static  type=wq_master | \
${DIR}/deltadb_reduce_temporal d30 workers,MAX total_tasks_dispatched,MAX task_running,MAX tasks_running,MAX | \
${DIR}/deltadb_pivot owner workers.MAX total_tasks_dispatched.MAX tasks_running.MAX task_running.MAX

# vim: set noexpandtab tabstop=4:
