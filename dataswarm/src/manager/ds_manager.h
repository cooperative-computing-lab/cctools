/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_MANAGER_H
#define DS_MANAGER_H

#include "dataswarm.h"
#include <limits.h>

typedef enum {
	DS_SUCCESS = 0,
	DS_WORKER_FAILURE,
	DS_APP_FAILURE,
	DS_MGR_FAILURE,
	DS_END_OF_LIST,
} ds_result_code_t;

typedef enum {
	DS_MSG_PROCESSED = 0,        /* Message was processed and connection is still good. */
	DS_MSG_PROCESSED_DISCONNECT, /* Message was processed and disconnect now expected. */
	DS_MSG_NOT_PROCESSED,        /* Message was not processed, waiting to be consumed. */
	DS_MSG_FAILURE               /* Message not received, connection failure. */
} ds_msg_code_t;

typedef enum {
	DS_MON_DISABLED = 0,
	DS_MON_SUMMARY  = 1,   /* generate only summary. */
	DS_MON_FULL     = 2,   /* generate summary, series and monitoring debug output. */
	DS_MON_WATCHDOG = 4    /* kill tasks that exhaust resources */
} ds_monitoring_mode_t;

typedef enum {
	DS_WORKER_DISCONNECT_UNKNOWN  = 0,
	DS_WORKER_DISCONNECT_EXPLICIT,
	DS_WORKER_DISCONNECT_STATUS_WORKER,
	DS_WORKER_DISCONNECT_IDLE_OUT,
	DS_WORKER_DISCONNECT_FAST_ABORT,
	DS_WORKER_DISCONNECT_FAILURE
} ds_worker_disconnect_reason_t;

typedef enum {
	CORES_BIT = (1 << 0),
	MEMORY_BIT = (1 << 1),
	DISK_BIT = (1 << 2),
	GPUS_BIT = (1 << 3),
} ds_resource_bitmask_t;

struct ds_worker_info;
struct ds_task;
struct ds_file;

struct ds_manager {

	/* Connection and communication settings */

	char *name;          /* Project name describing this manager at the catalog server. */
	int   port;          /* Port number on which this manager is listening for connections. */
	int   priority;      /* Priority of this manager relative to other managers with the same name. */
	char *catalog_hosts; /* List of catalogs to which this manager reports. */
	char *manager_preferred_connection; /* Recommended method for connecting to this manager.  @ref ds_manager_preferred_connection */
	char  workingdir[PATH_MAX];         /* Current working dir, for reporting to the catalog server. */

	struct link *manager_link;       /* Listening TCP connection for accepting new workers. */
	struct link_info *poll_table;    /* Table for polling on all connected workers. */
	int poll_table_size;             /* Number of entries in poll_table. */

	/* Security configuration */

	char *password;      /* Shared secret between manager and worker. Usable with or without SSL. */
	char *ssl_key;       /* Filename of SSL private key */
	char *ssl_cert;      /* Filename of SSL certificate. */
	int   ssl_enabled;   /* If true, SSL transport is used between manager and workers */

	/* Primary data structures for tracking task state. */

	struct itable *tasks;           /* Maps taskid -> ds_task of all tasks in any state. */
	struct itable *task_state_map;  /* Maps taskid -> ds_task_state_t */
	struct list   *ready_list;      /* List of ds_task that are waiting to execute. */
	struct list   *task_reports;    /* List of last N ds_task_reports for computing capacity. */
	struct hash_table *categories;  /* Maps category_name -> struct category */

	/* Primary data structures for tracking worker state. */

	struct hash_table *worker_table;     /* Maps link -> ds_worker_info */
	struct hash_table *worker_blocklist; /* Maps hostname -> ds_blocklist_info */
	struct itable     *worker_task_map;  /* Maps taskid -> ds_worker_info */     
	struct hash_table *factory_table;    /* Maps factory_name -> ds_factory_info */
	struct hash_table *workers_with_available_results;  /* Maps link -> ds_worker_info */

	/* Primary scheduling controls. */

	ds_schedule_t worker_selection_algorithm;    /* Mode for selecting best worker for task in main scheduler. */
	ds_category_mode_t allocation_default_mode;  /* Mode for computing resources allocations for each task. */

	/* Internal state modified by the manager */

 	int next_taskid;       /* Next integer taskid to be assigned to a created task. */
	int num_tasks_left;    /* Optional: Number of tasks remaining, if given by user.  @ref ds_specify_num_tasks */
	int busy_waiting_flag; /* Set internally in main loop if no messages were processed -> wait longer. */

	/* Accumulation of statistics for reporting to the caller. */

	struct ds_stats *stats;		 
	struct ds_stats *stats_measure;
	struct ds_stats *stats_disconnected_workers;

	/* Time of most recent events for computing various timeouts */

	timestamp_t time_last_wait;
	timestamp_t time_last_log_stats;
	timestamp_t time_last_large_tasks_check;
	timestamp_t link_poll_end;
	time_t      catalog_last_update_time;
	time_t      resources_last_update_time;

	/* Logging configuration. */

	FILE *perf_logfile; /* Performance logfile for tracking metrics by time. */
	FILE *txn_logfile;  /* Transaction logfile for recording every event of interest. */

	/* Resource monitoring configuration. */

	ds_monitoring_mode_t monitor_mode;
	FILE *monitor_file;
	char *monitor_output_directory;
	char *monitor_summary_filename;
	char *monitor_exe;

	struct rmsummary *measured_local_resources;
	struct rmsummary *current_max_worker;
	struct rmsummary *max_task_resources_requested;

	/* Various performance knobs that can be tuned. */

	int short_timeout;            /* Timeout in seconds to send/recv a brief message from worker */
	int long_timeout;             /* Timeout if in the middle of an incomplete message. */
	int minimum_transfer_timeout; /* Minimum number of seconds to allow for a manager<-> worker file transfer. */
	int transfer_outlier_factor;  /* Factor to consider a given transfer time to be an outlier justifying cancellation. */
	int default_transfer_rate;    /* Assumed data transfer rate for computing timeouts, prior to collecting observations. */
	int process_pending_check;    /* Enables check for waiting processes in main loop via @ref process_pending */
	int keepalive_interval;	      /* Time between keepalive request transmissions. */
	int keepalive_timeout;	      /* Keepalive response must be received within this time, otherwise worker disconnected. */
	int hungry_minimum;           /* Minimum number of waiting tasks to consider queue not hungry. */
	int wait_for_workers;         /* wait for these many workers to connect before dispatching tasks at start of execution. */
	int fetch_factory;            /* If true, manager queries catalog for factory configuration. */
	int wait_retrieve_many;       /* If true, main loop consumes multiple completed tasks at once. */
	int force_proportional_resources;  /* If true, tasks divide worker resources proportionally. */
	double resource_submit_multiplier; /* Factor to permit overcommitment of resources at each worker.  */
	double bandwidth_limit;            /* Artificial limit on bandwidth of manager<->worker transfers. */
	int disk_avail_threshold; /* Ensure this minimum amount of available disk space. (in MB */
};

/* Internal interfaces to ds_manager.c */

#ifndef SWIG
__attribute__ (( format(printf,3,4) ))
#endif
int ds_manager_send( struct ds_manager *q, struct ds_worker_info *w, const char *fmt, ... );
ds_msg_code_t ds_manager_recv_retry( struct ds_manager *q, struct ds_worker_info *w, char *line, int length );

int ds_manager_transfer_wait_time( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, int64_t length );
int ds_manager_available_workers(struct ds_manager *q);

const struct rmsummary *task_min_resources(struct ds_manager *q, struct ds_task *t);

#define RESOURCE_MONITOR_TASK_LOCAL_NAME "ds-%d-task-%d"
#define RESOURCE_MONITOR_REMOTE_NAME "cctools-monitor"
#define RESOURCE_MONITOR_REMOTE_NAME_EVENTS RESOURCE_MONITOR_REMOTE_NAME "events.json"

#endif
