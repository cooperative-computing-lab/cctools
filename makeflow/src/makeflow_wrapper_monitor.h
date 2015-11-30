
/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_WRAPPER_MONITOR_H
#define MAKEFLOW_WRAPPER_MONITOR_H

/*
This module implements garbage collection on the dag.
Files that are no longer needed as inputs to any rules
may be removed, according to a variety of criteria.
*/

struct makeflow_monitor {
	struct makeflow_wrapper *wrapper;
	int enable_debug;
	int enable_time_series;
	int enable_list_files;

	int interval;
	char *log_prefix;
	struct rmsummary *limits;
	char *exe;
	const char *exe_remote;
};

struct makeflow_monitor * makeflow_monitor_create();
void makeflow_prepare_for_monitoring( struct makeflow_monitor *m, struct batch_queue *queue, char *log_dir, char *log_format);
char *makeflow_wrap_monitor( char *result, struct dag_node *n, struct makeflow_monitor *m );

#endif
