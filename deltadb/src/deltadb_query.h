#ifndef DELTADB_QUERY_H
#define DELTADB_QUERY_H

#include <stdio.h>
#include <time.h>

#include "deltadb_reduction.h"
#include "hash_table.h"
#include "jx.h"
#include "list.h"

typedef enum {
  DELTADB_DISPLAY_STREAM,
  DELTADB_DISPLAY_OBJECTS,
  DELTADB_DISPLAY_EXPRS,
  DELTADB_DISPLAY_REDUCE
} deltadb_display_mode_t;

struct deltadb_query * deltadb_query_create();

void deltadb_query_delete( struct deltadb_query *q );

void deltadb_query_set_display( struct deltadb_query *q, deltadb_display_mode_t mode );
void deltadb_query_set_filter( struct deltadb_query *q, struct jx *expr );
void deltadb_query_set_where( struct deltadb_query *q, struct jx *expr );
void deltadb_query_set_epoch_mode( struct deltadb_query *q, int mode );
void deltadb_query_set_interval( struct deltadb_query *q, int interval );
void deltadb_query_set_output( struct deltadb_query *q, FILE *stream );

void deltadb_query_add_output( struct deltadb_query *q, struct jx *expr );
void deltadb_query_add_reduction( struct deltadb_query *q, struct deltadb_reduction *reduce );

int deltadb_query_execute_dir( struct deltadb_query *q, const char *dir, time_t starttime, time_t stoptime );
int deltadb_query_execute_stream( struct deltadb_query *q, FILE *stream, time_t starttime, time_t stoptime );

#endif
