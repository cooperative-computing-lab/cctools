/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_MANAGER_H
#define VINE_MANAGER_H

/*
This module defines the structures and types of the manager process as a whole.
This module is private to the manager and should not be invoked by the end user.
*/

#include "taskvine.h"
#include <limits.h>

/*
The result of a variety of internal operations, indicating whether
the operation succeeded, or failed due to the fault of the worker,
the application, or the manager.
*/

typedef enum {
	VINE_SUCCESS = 0,
	VINE_WORKER_FAILURE,
	VINE_APP_FAILURE,
	VINE_MGR_FAILURE,
	VINE_END_OF_LIST,
} vine_result_code_t;

/*
The result of vine_manager_recv{_no_retry}, indicating whether an
incoming message was processed, and the expected next state of the connection.
*/

typedef enum {
	VINE_MSG_PROCESSED = 0,        /* Message was processed and connection is still good. */
	VINE_MSG_PROCESSED_DISCONNECT, /* Message was processed and disconnect now expected. */
	VINE_MSG_NOT_PROCESSED,        /* Message was not processed, waiting to be consumed. */
	VINE_MSG_FAILURE               /* Message not received, connection failure. */
} vine_msg_code_t;

/* The current resource monitoring configuration of the manager. */

typedef enum {
	VINE_MON_DISABLED = 0,
	VINE_MON_SUMMARY  = 1,   /* generate only summary. */
	VINE_MON_FULL     = 2,   /* generate summary, series and monitoring debug output. */
	VINE_MON_WATCHDOG = 4    /* kill tasks that exhaust resources */
} vine_monitoring_mode_t;

/* The various reasons why a worker process may disconnect from the manager. */

typedef enum {
	VINE_WORKER_DISCONNECT_UNKNOWN  = 0,
	VINE_WORKER_DISCONNECT_EXPLICIT,
	VINE_WORKER_DISCONNECT_STATUS_WORKER,
	VINE_WORKER_DISCONNECT_IDLE_OUT,
 	VINE_WORKER_DISCONNECT_FAST_ABORT,
	VINE_WORKER_DISCONNECT_FAILURE
} vine_worker_disconnect_reason_t;

/* States known about libraries */

typedef enum {
	VINE_LIBRARY_WAITING = 0,
	VINE_LIBRARY_SENT,
	VINE_LIBRARY_STARTED,
	VINE_LIBRARY_FAILURE
} vine_library_state_t;

struct vine_worker_info;
struct vine_task;
struct vine_file;

struct vine_manager {

	/* Connection and communication settings */

	char *name;          /* Project name describing this manager at the catalog server. */
	int   port;          /* Port number on which this manager is listening for connections. */
	int   priority;      /* Priority of this manager relative to other managers with the same name. */
	char *catalog_hosts; /* List of catalogs to which this manager reports. */
	char *manager_preferred_connection; /* Recommended method for connecting to this manager.  @ref vine_set_manager_preferred_connection */
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

	struct itable *tasks;           /* Maps task_id -> vine_task of all tasks in any state. */
	struct list   *ready_list;      /* List of vine_task that are waiting to execute. */
	struct itable   *running_table;      /* Table of vine_task that are running at workers. */
	struct list   *waiting_retrieval_list;      /* List of vine_task that are waiting to be retrieved. */
	struct list   *retrieved_list;      /* List of vine_task that have been retrieved. */
	struct list   *task_info_list;  /* List of last N vine_task_infos for computing capacity. */
	struct hash_table *categories;  /* Maps category_name -> struct category */
	struct hash_table *libraries;      /* Maps library name -> vine_task of library with that name. */

	/* Primary data structures for tracking worker state. */

	struct hash_table *worker_table;     /* Maps link -> vine_worker_info */
	struct hash_table *worker_blocklist; /* Maps hostname -> vine_blocklist_info */
	struct hash_table *factory_table;    /* Maps factory_name -> vine_factory_info */
	struct hash_table *workers_with_available_results;  /* Maps link -> vine_worker_info */
	struct hash_table *current_transfer_table; 	/* Maps uuid -> struct transfer_pair */

	/* Primary data structures for tracking files. */

    	struct hash_table *file_table;      /* Maps fileid -> struct vine_file.* */

	/* Primary scheduling controls. */

	vine_schedule_t worker_selection_algorithm;    /* Mode for selecting best worker for task in main scheduler. */
	vine_category_mode_t allocation_default_mode;  /* Mode for computing resources allocations for each task. */

	/* Internal state modified by the manager */

	int next_task_id;       /* Next integer task_id to be assigned to a created task. */
	int num_tasks_left;    /* Optional: Number of tasks remaining, if given by user.  @ref vine_set_num_tasks */
	int busy_waiting_flag; /* Set internally in main loop if no messages were processed -> wait longer. */

	/* Accumulation of statistics for reporting to the caller. */

	struct vine_stats *stats;
	struct vine_stats *stats_measure;
	struct vine_stats *stats_disconnected_workers;

	/* Time of most recent events for computing various timeouts */

	timestamp_t time_last_wait;
	timestamp_t time_last_log_stats;
	timestamp_t time_last_large_tasks_check;
	timestamp_t link_poll_end;
	time_t      catalog_last_update_time;
	time_t      resources_last_update_time;

	/* Logging configuration. */

    char *runtime_directory;
	FILE *perf_logfile;        /* Performance logfile for tracking metrics by time. */
	FILE *txn_logfile;         /* Transaction logfile for recording every event of interest. */
	FILE *graph_logfile;       /* Graph logfile for visualizing application structure. */
	int perf_log_interval;	   /* Minimum interval for performance log entries in seconds. */
	
	/* Resource monitoring configuration. */

	vine_monitoring_mode_t monitor_mode;
	struct vine_file *monitor_exe;
    int monitor_interval;

	struct rmsummary *measured_local_resources;
	struct rmsummary *current_max_worker;
	struct rmsummary *max_task_resources_requested;

	/* Peer Transfer Configuration */
	int peer_transfers_enabled;
	int file_source_max_transfers;
	int worker_source_max_transfers;
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
	int wait_for_workers;         /* Wait for these many workers to connect before dispatching tasks at start of execution. */
	int attempt_schedule_depth;   /* number of submitted tasks to attempt scheduling before we continue to retrievals */
    int max_retrievals;           /* Do at most this number of task retrievals of either receive_one_task or receive_all_tasks_from_worker. If less
                                     than 1, prefer to receive all completed tasks before submitting new tasks. */
	int worker_retrievals;        /* retrieve all completed tasks from a worker as opposed to recieving one of any completed task*/
	int fetch_factory;            /* If true, manager queries catalog for factory configuration. */
	int proportional_resources;   /* If true, tasks divide worker resources proportionally. */
	int proportional_whole_tasks; /* If true, round-up proportions to whole number of tasks. */
	double resource_submit_multiplier; /* Factor to permit overcommitment of resources at each worker.  */
	double bandwidth_limit;            /* Artificial limit on bandwidth of manager<->worker transfers. */
	int disk_avail_threshold; /* Ensure this minimum amount of available disk space. (in MB) */
};

/*
These are not public API functions, but utility methods that may
be called on the manager object by other elements of the manager process.
*/

/* Declares file f. If a file with the same f->cached_name is already declared, f
 * is ****deleted**** and the previous file is returned. Otherwise f is returned. */
struct vine_file *vine_manager_declare_file(struct vine_manager *m, struct vine_file *f);
struct vine_file *vine_manager_lookup_file(struct vine_manager *q, const char *cached_name);

/* Send a printf-style message to a remote worker. */
#ifndef SWIG
__attribute__ (( format(printf,3,4) ))
#endif
int vine_manager_send( struct vine_manager *q, struct vine_worker_info *w, const char *fmt, ... );

/* Receive a line-oriented message from a remote worker. */
vine_msg_code_t vine_manager_recv( struct vine_manager *q, struct vine_worker_info *w, char *line, int length );

/* Compute the expected wait time for a transfer of length bytes. */
int vine_manager_transfer_time( struct vine_manager *q, struct vine_worker_info *w, int64_t length );

/* Various functions to compute expected properties of tasks. */
const struct rmsummary *vine_manager_task_resources_min(struct vine_manager *q, struct vine_task *t);
const struct rmsummary *vine_manager_task_resources_max(struct vine_manager *q, struct vine_task *t);

/* Internal: Enable shortcut of main loop upon child process completion. Needed for Makeflow to interleave local and remote execution. */
void vine_manager_enable_process_shortcut(struct vine_manager *q);

struct rmsummary *vine_manager_choose_resources_for_task( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t );

int64_t overcommitted_resource_total(struct vine_manager *q, int64_t total);


/* The expected format of files created by the resource monitor.*/
#define RESOURCE_MONITOR_TASK_LOCAL_NAME "vine-task-%d"
#define RESOURCE_MONITOR_REMOTE_NAME "cctools-monitor"
#define RESOURCE_MONITOR_REMOTE_NAME_EVENTS RESOURCE_MONITOR_REMOTE_NAME "events.json"

#endif
