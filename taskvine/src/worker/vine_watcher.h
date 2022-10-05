/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_WATCH_H
#define VINE_WATCH_H

#include "vine_process.h"
#include "link.h"

struct vine_watcher * vine_watcher_create();
void vine_watcher_delete( struct vine_watcher *w );

void vine_watcher_add_process( struct vine_watcher *w, struct vine_process *p );
void vine_watcher_remove_process( struct vine_watcher *w, struct vine_process *p );
int vine_watcher_check( struct vine_watcher *w );
int vine_watcher_send_changes( struct vine_watcher *w, struct link *manager, time_t stoptime );

#endif
