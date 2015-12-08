/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_stream.h"

#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"

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
	char *fields[100];
	int nfields;
	time_t previous_time;
};

struct deltadb * deltadb_create()
{
	struct deltadb *db = malloc(sizeof(*db));
	memset(db,0,sizeof(*db));
	db->table = hash_table_create(0,0);
	db->nfields = 0;
	return db;
}

void emit_table_header( struct deltadb *db )
{
	int i;

	printf("#time\t");
	for(i=0;i<db->nfields;i++) {
		printf("%s\t",db->fields[i]);
	}
	printf("\n");
}


void emit_table_values( struct deltadb *db, time_t current)
{
	char *key;
	struct jx *jobject;
	int i;

	hash_table_firstkey(db->table);
	while(hash_table_nextkey(db->table, &key, (void **) &jobject)) {
		printf("%ld\t",current);
		for(i=0;i<db->nfields;i++) {
			struct jx *jvalue = jx_lookup(jobject,db->fields[i]);
			if(!jvalue) {
				printf("null\t");
			} else {
				jx_print_stream(jvalue,stdout);
			       	printf("\t");
			}
		}
		printf("\n");
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
		emit_table_values(db,db->previous_time);
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
	if(argc<2) {
		fprintf(stderr,"use: deltadb_pivot [column1] [column2] ... [columnN]\n");
		return 1;
	}

	struct deltadb *db = deltadb_create();
	int i;

	for(i=1;i<argc;i++) {
		db->fields[db->nfields++] = argv[i];
	}

	emit_table_header(db);
	deltadb_process_stream(db,stdin,0,0);
	emit_table_values(db,db->previous_time);
	fflush(stdout);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
