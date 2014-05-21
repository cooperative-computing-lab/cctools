Here are three examples of queries that can be performed on the data:

./ddb_collect /var/tmp/catalog.history 2013-02-1@00:00:00 d365 | \
./ddb_select_static  type=wq_master | \
./ddb_reduce_temporal m15 workers,MAX task_running,MAX tasks_running,MAX | \
./ddb_reduce_spatial name,CNT workers.MAX,SUM task_running.MAX,SUM tasks_running.MAX,SUM | \
./ddb_pivot name.CNT workers.MAX.SUM task_running.MAX.SUM tasks_running.MAX.SUM



./ddb_collect /var/tmp/catalog.history 2013-02-1@00:00:00 d365 | \
./ddb_select_static  type=wq_master | \
./ddb_reduce_temporal d30 workers,MAX total_tasks_dispatched,MAX task_running,MAX tasks_running,MAX | \
./ddb_pivot owner workers.MAX total_tasks_dispatched.MAX tasks_running.MAX task_running.MAX



./ddb_collect /var/tmp/catalog.history 2013-03-14@00:00:00 d1 | \
./ddb_select_static  type=chirp | \
./ddb_project name | \
./ddb_reduce_temporal d1 name,LAST | \
./ddb_pivot name.LAST


