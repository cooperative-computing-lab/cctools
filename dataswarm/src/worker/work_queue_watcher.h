/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_WATCH_H
#define DS_WATCH_H

#include "ds_process.h"
#include "link.h"

struct ds_watcher * ds_watcher_create();
void ds_watcher_delete( struct ds_watcher *w );

void ds_watcher_add_process( struct ds_watcher *w, struct ds_process *p );
void ds_watcher_remove_process( struct ds_watcher *w, struct ds_process *p );
int ds_watcher_check( struct ds_watcher *w );
int ds_watcher_send_changes( struct ds_watcher *w, struct link *manager, time_t stoptime );

#endif
