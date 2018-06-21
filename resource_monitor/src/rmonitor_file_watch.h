/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef RMONITOR_FILE_WATCH

#include "sys/types.h"
#include "unistd.h"
#include "list.h"
#include "jx.h"

struct rmonitor_file_watch_info {
    const char  *filename;

    off_t position;

    off_t  last_size;
    time_t last_mtime;
    ino_t  last_ino;

    int from_start;
    int from_start_if_truncated;

    int exists;
    int delete_if_found;

    struct list *events;
    int event_with_pattern;
};

struct rmonitor_file_watch_event {
    char *label;

    int on_creation;
    int on_deletion;
    int on_truncate;

    char *on_pattern;

    jx_int_t max_count;      // max number of occurances to report event
    jx_int_t total_count;    // times the event has been found

    jx_int_t cycle_count;    // times the event has been found since last check
};

/* Checks file for events. Forks and returns the pid of the child. */
pid_t rmonitor_watch_file(const char *filename, struct jx *events_array);

#endif

