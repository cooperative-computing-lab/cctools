#ifndef DELTADB_QUERY_H
#define DELTADB_QUERY_h

#include <stdio.h>
#include <time.h>

#include "hash_table.h"
#include "jx.h"
#include "list.h"

typedef enum { MODE_STREAM, MODE_OBJECT, MODE_REDUCE } deltadb_display_mode_t;

struct deltadb {
	struct hash_table *table;
	const char *logdir;
	FILE *logfile;
	int epoch_mode;
	struct jx *filter_expr;
	struct jx *where_expr;
	struct list * output_exprs;
	struct list * reduce_exprs;
	time_t display_every;
	time_t display_next;
	time_t deferred_time;
	deltadb_display_mode_t display_mode;
};

struct deltadb * deltadb_create( const char *logdir );

int deltadb_query_execute( struct deltadb *db, time_t starttime, time_t stoptime );

#endif
