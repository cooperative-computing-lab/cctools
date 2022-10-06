/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DELTADB_STREAM_H
#define DELTADB_STREAM_H

#include "jx.h"

#include <stdio.h>
#include <time.h>

struct deltadb_query;

struct deltadb_event_handlers {
	int (*deltadb_create_event) ( struct deltadb_query *query, const char *key, struct jx *jobject );
	int (*deltadb_delete_event)( struct deltadb_query *query, const char *key );
	int (*deltadb_update_event) ( struct deltadb_query *query, const char *key, const char *name, struct jx *jvalue );
	int (*deltadb_merge_event) ( struct deltadb_query *query, const char *key, struct jx *jobject );
	int (*deltadb_remove_event) ( struct deltadb_query *query, const char *key, const char *name );
	int (*deltadb_time_event) ( struct deltadb_query *query, time_t starttime, time_t stoptime, time_t current );
	int (*deltadb_raw_event) ( struct deltadb_query *query, const char *line );
};

int deltadb_process_stream( struct deltadb_query *query, struct deltadb_event_handlers *handlers, FILE *stream, time_t starttime, time_t stoptime );

int deltadb_process_stream_fast( struct deltadb_query *query, struct deltadb_event_handlers *handlers, FILE *stream, time_t starttime, time_t stoptime );

#endif
