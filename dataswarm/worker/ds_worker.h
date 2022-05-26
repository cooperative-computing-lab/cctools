#ifndef DATASWARM_WORKER_H
#define DATASWARM_WORKER_H

#include <time.h>
#include "buffer.h"
#include "hash_table.h"
#include "link.h"
#include "mq.h"

#include "ds_resources.h"

struct ds_worker {
	// Network connection to the manager process.
	struct mq *manager_connection;

	// Table mapping taskids to ds_task objects.
	struct hash_table *task_table;

	// Table mapping taskids to ds_process objects representing running tasks.
	struct hash_table *process_table;

	// Table mapping blobids to ds_blob objects.
	struct hash_table *blob_table;

	// Path to top of workspace containing tasks and blobs.
	char *workspace;

	/* Current resources committed, in BYTES. */
	struct ds_resources *resources_inuse;

	/* Total resources available, in BYTES */
	struct ds_resources *resources_total;

	/***************************************************************/
	/* Internal tuning parameters set in ds_worker_create() */
	/***************************************************************/

	// Give up and reconnect if no message received after this time.
	int idle_timeout;

	// Abort a single message transmission if stuck for this long.
	int long_timeout;

	// Minimum time between connection attempts.
	int min_connect_retry;

	// Maximum time between connection attempts.
	int max_connect_retry;

	// Maximum time to wait for a catalog query
	int catalog_timeout;

	// Time last status update was sent to manager.
	time_t last_status_report;

	// Seconds between updates.
	int status_report_interval;

	// Place to store messages
	buffer_t recv_buffer;
};

struct ds_worker *ds_worker_create();
void ds_worker_delete(struct ds_worker *w);

void ds_worker_connect_by_name( struct ds_worker *w, const char *manager_name );
void ds_worker_connect_loop( struct ds_worker *w, const char *manager_host, int manager_port );

void ds_worker_measure_resources( struct ds_worker *w );
int  ds_worker_resources_avail( struct ds_worker *w, struct ds_resources *r );
void ds_worker_resources_alloc( struct ds_worker *w, struct ds_resources *r );
void ds_worker_resources_free_except_disk( struct ds_worker *w, struct ds_resources *r );

int  ds_worker_disk_avail( struct ds_worker *w, int64_t size );
void ds_worker_disk_alloc( struct ds_worker *w, int64_t size );
void ds_worker_disk_free( struct ds_worker *w, int64_t size );

char * ds_worker_task_dir( struct ds_worker *w, const char *taskid );
char * ds_worker_task_sandbox( struct ds_worker *w, const char *taskid );
char * ds_worker_task_meta( struct ds_worker *w, const char *taskid );
char * ds_worker_task_deleting( struct ds_worker *w );

char * ds_worker_blob_dir( struct ds_worker *w, const char *blobid );
char * ds_worker_blob_data( struct ds_worker *w, const char *blobid );
char * ds_worker_blob_meta( struct ds_worker *w, const char *blobid );
char * ds_worker_blob_deleting( struct ds_worker *w );

#endif
