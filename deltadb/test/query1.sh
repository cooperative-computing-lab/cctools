#!/bin/sh

# List the total number of queues, workers, and tasks
# at 15 minute intervals over the course of one year.



DIR=${CCTOOLS}/bin
DATA=/var/tmp/catalog.history


${DIR}/ddb_collect ${DATA} 2013-02-1@00:00:00 d365 | \
${DIR}/ddb_select_static  type=wq_master | \
${DIR}/ddb_reduce_temporal m15 workers,MAX task_running,MAX tasks_running,MAX | \
${DIR}/ddb_reduce_spatial name,CNT workers.MAX,SUM task_running.MAX,SUM tasks_running.MAX,SUM | \
${DIR}/ddb_pivot name.CNT workers.MAX.SUM task_running.MAX.SUM tasks_running.MAX.SUM
