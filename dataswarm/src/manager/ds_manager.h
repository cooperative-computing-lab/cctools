/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_MANAGER_H
#define DS_MANAGER_H

#include "dataswarm.h"
#include <limits.h>

struct ds_manager {
	char *name;
	int port;
	int priority;
	int num_tasks_left;

	int next_taskid;

	char workingdir[PATH_MAX];

	struct link      *manager_link;   // incoming tcp connection for workers.
	struct link_info *poll_table;
	int poll_table_size;

	struct itable *tasks;           // taskid -> task
	struct itable *task_state_map;  // taskid -> state
	struct list   *ready_list;      // ready to be sent to a worker

	struct hash_table *worker_table;
	struct hash_table *worker_blocklist;
	struct itable  *worker_task_map;

	struct hash_table *factory_table;

	struct hash_table *categories;

	struct hash_table *workers_with_available_results;

	struct ds_stats *stats;
	struct ds_stats *stats_measure;
	struct ds_stats *stats_disconnected_workers;
	timestamp_t time_last_wait;
	timestamp_t time_last_log_stats;
	timestamp_t time_last_large_tasks_check;
	int worker_selection_algorithm;
	int process_pending_check;

	int short_timeout;		// timeout to send/recv a brief message from worker
	int long_timeout;		// timeout if in the middle of an incomplete message.
	struct list *task_reports;	      /* list of last N ds_task_reports. */

	double resource_submit_multiplier; /* Times the resource value, but disk */

	int minimum_transfer_timeout;
	int transfer_outlier_factor;
	int default_transfer_rate;

	char *catalog_hosts;

	time_t catalog_last_update_time;
	time_t resources_last_update_time;
	int    busy_waiting_flag;

	int hungry_minimum;               /* minimum number of waiting tasks to consider queue not hungry. */;

	int wait_for_workers;             /* wait for these many workers before dispatching tasks at start of execution. */

	ds_category_mode_t allocation_default_mode;

	FILE *logfile;
	FILE *transactions_logfile;
	int keepalive_interval;
	int keepalive_timeout;
	timestamp_t link_poll_end;	//tracks when we poll link; used to timeout unacknowledged keepalive checks

    char *manager_preferred_connection;

	int monitor_mode;
	FILE *monitor_file;

	char *monitor_output_directory;
	char *monitor_summary_filename;

	char *monitor_exe;
	struct rmsummary *measured_local_resources;
	struct rmsummary *current_max_worker;
	struct rmsummary *max_task_resources_requested;

	char *password;
	char *ssl_key;
	char *ssl_cert;
	int ssl_enabled;

	double bandwidth;

	int fetch_factory;

	int wait_retrieve_many;
	int force_proportional_resources;
};

void resource_monitor_append_report(struct ds_manager *q, struct ds_task *t);

#endif
