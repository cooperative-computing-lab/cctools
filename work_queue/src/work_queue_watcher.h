/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef WORK_QUEUE_WATCH_H
#define WORK_QUEUE_WATCH_H

#include "work_queue_process.h"
#include "link.h"

struct work_queue_watcher * work_queue_watcher_create();
void work_queue_watcher_delete( struct work_queue_watcher *w );

void work_queue_watcher_add_process( struct work_queue_watcher *w, struct work_queue_process *p );
void work_queue_watcher_remove_process( struct work_queue_watcher *w, struct work_queue_process *p );
int work_queue_watcher_check( struct work_queue_watcher *w );
int work_queue_watcher_send_changes( struct work_queue_watcher *w, struct link *manager, time_t stoptime );

#endif
