#ifndef DELTADB_QUERY_H
#define DELTADB_QUERY_h

#include <stdio.h>
#include <time.h>

#include "deltadb_reduction.h"
#include "hash_table.h"
#include "jx.h"
#include "list.h"

typedef enum {
  DELTADB_DISPLAY_STREAM,
  DELTADB_DISPLAY_OBJECT,
  DELTADB_DISPLAY_REDUCE
} deltadb_display_mode_t;

struct deltadb * deltadb_create();

void deltadb_query_set_display( struct deltadb *db, deltadb_display_mode_t mode );
void deltadb_query_set_filter( struct deltadb *db, struct jx *expr );
void deltadb_query_set_where( struct deltadb *db, struct jx *expr );
void deltadb_query_set_epoch_mode( struct deltadb *db, int mode );
void deltadb_query_set_interval( struct deltadb *db, int interval );

void deltadb_query_add_output( struct deltadb *db, struct jx *expr );
void deltadb_query_add_reduction( struct deltadb *db, struct deltadb_reduction *reduce );

int deltadb_query_execute_dir( struct deltadb *db, const char *dir, time_t starttime, time_t stoptime );
int deltadb_query_execute_stream( struct deltadb *db, FILE *stream, time_t starttime, time_t stoptime );

void deltadb_query_delete( struct deltadb *db );

#endif
