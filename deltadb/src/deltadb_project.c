/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_stream.h"

#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>


struct deltadb {
	char **attr;
	int nattr;
	time_t current;
	time_t lastprint;
};

struct deltadb * deltadb_create( )
{
	struct deltadb *db = malloc(sizeof(*db));
	memset(db,0,sizeof(*db));
	return db;
}

static int in_attr( struct deltadb *db, const char *name )
{
	int i;
	for(i=0;i<db->nattr;i++) {
		if(!strcmp(name,db->attr[i])) return 1;
	}
	return 0;
}

static void print_time_if_changed( struct deltadb *db )
{
	if(db->lastprint!=db->current) {
		printf("T %lld\n",(long long)db->current);
		db->lastprint = db->current;
	}
}

int deltadb_create_event( struct deltadb *db, const char *key, struct jx *jobject )
{
	print_time_if_changed(db);

	struct jx *j = jx_object(0);
	int i;

	for(i=0;i<db->nattr;i++) {
		struct jx * jvalue = jx_lookup(jobject,db->attr[i]);
		if(jvalue) {
			jx_insert(j,jx_string(db->attr[i]),jx_copy(jvalue));
		}
	}

	char *str = jx_print_string(j);
	printf("C %s %s\n",key,str);
	free(str);
	jx_delete(j);

	return 1;
}

int deltadb_delete_event( struct deltadb *db, const char *key )
{
	print_time_if_changed(db);

	printf("D %s\n",key);
	return 1;
}

int deltadb_update_event( struct deltadb *db, const char *key, const char *name, struct jx *jvalue )
{
	if(in_attr(db,name)) {
		print_time_if_changed(db);
		char *str = jx_print_string(jvalue);
		printf("U %s %s %s\n",key,name,str);
		free(str);
	}	
	return 1;
}

int deltadb_remove_event( struct deltadb *db, const char *key, const char *name )
{
	if(in_attr(db,name)) {
		print_time_if_changed(db);
		printf("R %s %s\n",key,name);
	}
	return 1;
}

int deltadb_time_event( struct deltadb *db, time_t starttime, time_t stoptime, time_t current )
{
	db->current = current;
	return 1;
}

int deltadb_post_event( struct deltadb *db, const char *line )
{
	return 1;
}


int main( int argc, char *argv[] )
{
	struct deltadb *db = deltadb_create();

	int i;
	db->nattr = argc-1;
	db->attr = malloc(sizeof(char*)*db->nattr);
	for (i=1; i<argc; i++){
		db->attr[i-1] = argv[i];
	}

	deltadb_process_stream(db,stdin,0,0);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
