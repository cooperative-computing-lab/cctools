/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "nvpair.h"
#include "hash_table.h"
#include "text_list.h"
#include "debug.h"
#include "limits.h"

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
};

struct deltadb * deltadb_create()
{
	struct deltadb *db = malloc(sizeof(*db));
	memset(db,0,sizeof(*db));
	db->table = hash_table_create(0,0);
	db->nfields = 0;
	return db;
}

void deltadb_delete( struct deltadb *db )
{
	if(db->table) hash_table_delete(db->table);
	free(db);
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
	struct nvpair *nv;
	int i;

	hash_table_firstkey(db->table);
	while(hash_table_nextkey(db->table, &key, (void **) &nv)) {
		printf("%ld\t",current);
		for(i=0;i<db->nfields;i++) {
			const char *value = nvpair_lookup_string(nv,db->fields[i]);
			if(!value) value = "null";
			printf("%s\t",value);
		}
		printf("\n");
	}
}


/*
Replay a given log file into the hash table, up to the given snapshot time.
Return true if the stoptime was reached.
*/

#define NVPAIR_LINE_MAX 1024

static int log_play( struct deltadb *db, FILE *stream )
{
	time_t current = 0;
	time_t previous_time = 0;
	int line_number = 0;

	char line[NVPAIR_LINE_MAX];
	char key[NVPAIR_LINE_MAX];
	char name[NVPAIR_LINE_MAX];
	char value[NVPAIR_LINE_MAX];
	char oper;

	while(fgets(line,sizeof(line),stream)) {
		//debug(D_NOTICE,"Processed line: %s",line);
		line_number += 1;

		if (line[0]=='\n') break;

		int n = sscanf(line,"%c %s %s %[^\n]",&oper,key,name,value);
		if(n<1) continue;

		struct nvpair *nv;

		switch(oper) {
			case 'C':
				nv = nvpair_create();
				int num_pairs = nvpair_parse_stream(nv,stream);
				if(num_pairs>0) {
					nvpair_delete(hash_table_remove(db->table,key));
					hash_table_insert(db->table,key,nv);
				} else if (num_pairs == -1) {
					nvpair_delete(nv);
					break;
				} else {
					nvpair_delete(nv);
				}


				break;
			case 'D':
				nv = hash_table_remove(db->table,key);
				if(nv) nvpair_delete(nv);
				break;
			case 'U':
				nv = hash_table_lookup(db->table,key);
				if(nv) nvpair_insert_string(nv,name,value);
				break;
			case 'R':
				nv = hash_table_lookup(db->table,key);
				if(nv) nvpair_remove(nv,name);
				break;
			case 'T':
				previous_time = current;
				current = atol(key);
				emit_table_values(db,previous_time);
				break;
			default:
				debug(D_NOTICE,"corrupt log data[%i]: %s",line_number,line);
				fflush(stderr);
				break;
		}
	}
	emit_table_values(db,current);
	return 1;
}

int main( int argc, char *argv[] )
{
	struct deltadb *db = deltadb_create();
	int i;

	for(i=1;i<argc;i++) {
		db->fields[db->nfields++] = argv[i];
	}

	emit_table_header(db);
	log_play(db,stdin);

	deltadb_delete(db);

	return 0;
}
