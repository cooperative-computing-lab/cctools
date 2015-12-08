/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"

#include "deltadb_stream.h"
#include "deltadb_reduction.h"

#include "hash_table.h"
#include "debug.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

struct deltadb {
	struct hash_table *table;
	struct deltadb_reduction *deltadb_reductions[100];
	int ndeltadb_reductions;
	time_t previous_time;
	int first_output;
};

struct deltadb * deltadb_create()
{
	struct deltadb *db = malloc(sizeof(*db));
	db->table = hash_table_create(0,0);
	db->ndeltadb_reductions = 0;
	db->previous_time = 0;
	db->first_output = 1;
	return db;
}

#define NVPAIR_LINE_MAX 1024

void emit_all_deltadb_reductions( struct deltadb *db, time_t current )
{
	int i;
	struct jx *jobject;
	char *key;

	/* Reset all deltadb_reduction state. */
	for(i=0;i<db->ndeltadb_reductions;i++) {
		deltadb_reduction_reset(db->deltadb_reductions[i]);
	}

	/* After each event, iterate over all objects... */

	hash_table_firstkey(db->table);
	while(hash_table_nextkey(db->table,&key,(void**)&jobject)) {

		/* Update all deltadb_reductions for that object. */
		for(i=0;i<db->ndeltadb_reductions;i++) {
			struct deltadb_reduction *r = db->deltadb_reductions[i];
			struct jx *jvalue = jx_lookup(jobject,r->attr);

			double value = 0;

			if(jvalue->type==JX_DOUBLE) {
				value = jvalue->double_value;
			} else if(jvalue->type==JX_INTEGER) {
				value = jvalue->integer_value;
			}

			deltadb_reduction_update(r,value);
		}
	}

	printf("T %ld\n",(long)current);

	if(db->first_output) {
		/* The first time we do this, make it a checkpoint record. */
		printf("C 0 {");
		for(i=0;i<db->ndeltadb_reductions;i++) {
			struct deltadb_reduction *r = db->deltadb_reductions[i];
			deltadb_reduction_print_json(r);
			if(i!=(db->ndeltadb_reductions-1)) printf(",");
		}
		printf("}\n");
		db->first_output = 0;
	} else {
		/* After that, make it an update record. */
		for(i=0;i<db->ndeltadb_reductions;i++) {
			struct deltadb_reduction *r = db->deltadb_reductions[i];
			printf("U 0 ");
			deltadb_reduction_print(r);
			printf("\n");
		}
	}
}

int deltadb_create_event( struct deltadb *db, const char *key, struct jx *jobject )
{
	hash_table_insert(db->table,key,jobject);
	return 1;
}

int deltadb_delete_event( struct deltadb *db, const char *key )
{
	jx_delete(hash_table_remove(db->table,key));
	return 1;
}

int deltadb_update_event( struct deltadb *db, const char *key, const char *name, struct jx *jvalue )
{
	struct jx *jobject;
	jobject = hash_table_lookup(db->table,key);
	if(jobject) {
		struct jx *jname = jx_string(name);
		jx_delete(jx_remove(jobject,jname));
		jx_insert(jobject,jname,jvalue);
	} else {
		jx_delete(jvalue);
	}
	return 1;
}

int deltadb_remove_event( struct deltadb *db, const char *key, const char *name )
{
	struct jx *jobject;
	jobject = hash_table_lookup(db->table,key);
	if(jobject) {
		struct jx *jname = jx_string(name);
		jx_delete(jx_remove(jobject,jname));
		jx_delete(jname);
	}
	return 1;
}

int deltadb_time_event( struct deltadb *db, time_t starttime, time_t stoptime, time_t current )
{
	if(db->previous_time) {
		emit_all_deltadb_reductions(db,db->previous_time);
	}
	db->previous_time = current;
	return 1;
}

int deltadb_post_event( struct deltadb *db, const char *line )
{
	return 1;
}

int main( int argc, char *argv[] )
{
	int i;

	struct deltadb *db = deltadb_create();

	for (i=1; i<argc; i++){
		char *attr = strtok(argv[i], ",");
		char *type = strtok(0,",");

		struct deltadb_reduction *r = deltadb_reduction_create(type,attr);

		if(!r) {
			fprintf(stderr,"%s: invalid deltadb_reduction: %s\n",argv[0],type);
			return 1;
		}

		db->deltadb_reductions[db->ndeltadb_reductions++] = r;
	}

	deltadb_process_stream(db,stdin,0,0);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
