/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef WORK_QUEUE_WATCH_H
#define WORK_QUEUE_WATCH_H

#include "link.h"

struct work_queue_watch * work_queue_watch_create();
void work_queue_watch_delete( struct work_queue_watch *w );

void work_queue_watch_add_file( struct work_queue_watch *w, const char *path );
void work_queue_watch_remove_file( struct work_queue_watch *w, const char *path );
int work_queue_watch_check( struct work_queue_watch *w );
int work_queue_watch_send_changes( struct work_queue_watch *w, struct link *master, time_t stoptime );

#endif
