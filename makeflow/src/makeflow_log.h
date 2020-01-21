/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_LOG_H
#define MAKEFLOW_LOG_H

#include "batch_file.h"
#include "dag.h"
#include "makeflow_gc.h"
#include "makeflow_alloc.h"
#include "timestamp.h"
#include "list.h"

/*
Each dag is associated with a log file that records each operation that
moves the workload forward.  As a node changes state, an event is written
to the log.  Upon recovery from a crash, makeflow_log_recover plays back
the state to recover the dag.
*/

void makeflow_log_started_event( struct dag *d );
void makeflow_log_aborted_event( struct dag *d );
void makeflow_log_failed_event( struct dag *d );
void makeflow_log_completed_event( struct dag *d );
void makeflow_log_event( struct dag *d, char *name, uint64_t value);
void makeflow_log_state_change( struct dag *d, struct dag_node *n, int newstate );
void makeflow_log_file_state_change( struct dag *d, struct dag_file *f, int newstate );
void makeflow_log_batch_file_state_change( struct dag *d, struct batch_file *f, int newstate );
void makeflow_log_dag_file_list_state_change( struct dag *d, struct list *fl, int newstate );
void makeflow_log_batch_file_list_state_change( struct dag *d, struct list *fl, int newstate );
void makeflow_log_alloc_event( struct dag *d, struct makeflow_alloc *alloc );
void makeflow_log_gc_event( struct dag *d, int collected, timestamp_t elapsed, int total_collected );
void makeflow_log_close(struct dag *d );

/* return 0 on success, return non-zero on failure. */
int makeflow_log_recover( struct dag *d, const char *filename, int verbose_mode, struct batch_queue *queue, makeflow_clean_depth clean_mode );

/* write the info of a dependency specified in the mountfile into the logging system
 * @param d: a dag structure
 * @param target: the target field specified in the mountfile
 * @param source: the source field specified in the mountfile
 * @param cache_name: the filename of the dependency in the cache
 * @param type: the mount type
 */
void makeflow_log_mount_event( struct dag *d, const char *target, const char *source, const char *cache_name, dag_file_source_t type );

/* write the dirname of the cache into the logging system
 * @param d: a dag structure
 * @param cache_dir: the dirname of the cache used to store the dependencies specified in a mountfile
 */
void makeflow_log_cache_event( struct dag *d, const char *cache_dir );

#endif
