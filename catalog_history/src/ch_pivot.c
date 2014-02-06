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
	struct hash_table *fields;
	int field_cnt;
};

struct deltadb * deltadb_create()
{
	struct deltadb *db = malloc(sizeof(*db));
	db->table = hash_table_create(0,0);
	db->fields = hash_table_create(0,0);
	int *v = malloc(sizeof(int*));
	*v = 0;
	hash_table_insert(db->fields,"key",v);
	db->field_cnt = 1;
	return db;
}

void deltadb_delete( struct deltadb *db )
{
	if(db->table) hash_table_delete(db->table);
	if(db->fields) hash_table_delete(db->fields);
	free(db);
}


#define NVPAIR_LINE_MAX 1024




static int checkpoint_read( struct deltadb *db )
{
	FILE * stream = stdin;
	if(!stream) return 0;
	while(1) {
		struct nvpair *nv = nvpair_create();
		int num_pairs = nvpair_parse_stream(nv,stream);
		if(num_pairs>0) {
			const char *key = nvpair_lookup_string(nv,"key");
			if(key) {
				nvpair_delete(hash_table_remove(db->table,key));
				hash_table_insert(db->table,key,nv);
				nvpair_print_text2(db->fields,&db->field_cnt,nv,stdout,0,NULL);
				
			} else debug(D_NOTICE,"no key in object create.");
		} else if (num_pairs == -1) {
			nvpair_delete(nv);
			return 1;
		} else {
			nvpair_delete(nv);
		}
	}
	return 1;
}

/*
Replay a given log file into the hash table, up to the given snapshot time.
Return true if the stoptime was reached.
*/

static int log_play( struct deltadb *db  )
{
	FILE *stream = stdin;
	time_t current = 0;
	int line_number = 0;
	//struct hash_table *table = db->table;

	char line[NVPAIR_LINE_MAX];
	char key[NVPAIR_LINE_MAX];
	char name[NVPAIR_LINE_MAX];
	char value[NVPAIR_LINE_MAX];
	char oper;
	char *keyp;
	void *nvp;
	
	while(fgets(line,sizeof(line),stream)) {
		//printf("(%s",line);
		//fflush(stdout);

		line_number += 1;
		
		if (line[0]=='.') return 0;
		
		int n = sscanf(line,"%c %s %s %[^\n]",&oper,key,name,value);
		if(n<1) continue;
		
		struct nvpair *nv;
		
		//int include;
		switch(oper) {
			case 'C':
				nv = nvpair_create();
				int num_pairs = nvpair_parse_stream(nv,stream);
				if(num_pairs>0) {
					nvpair_delete(hash_table_remove(db->table,key));
					hash_table_insert(db->table,key,nv);
				} else if (num_pairs == -1) {
					nvpair_delete(nv);
					return 1;
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

				hash_table_firstkey(db->table);
				while(hash_table_nextkey(db->table, &keyp, &nvp)) {
					struct nvpair *nv = nvp;
					nvpair_print_text2(db->fields,&db->field_cnt,nv,stdout,current,keyp);
				}



				current = atol(key);
				break;
			default:
				debug(D_NOTICE,"corrupt log data[%i]: %s",line_number,line);
				fflush(stderr);
				break;
		}
	}
	return 1;
}



/*
Play the log from start_time to end_time by opening the appropriate
checkpoint file and working ahead in the various log files.
*/
struct silly2 {
	int pos;
};
struct silly2 * silly2_create(){
	struct silly2 *s = malloc(sizeof(*s));
	return s;
}

static int parse_input( struct deltadb *db )
{      
	checkpoint_read(db);
	log_play(db);

	int i;
	struct silly2 *s;
	char *key;
	void *value;
	printf("#Time");
	for(i=0;i<db->field_cnt;i++){
		hash_table_firstkey(db->fields);
		while(hash_table_nextkey(db->fields,&key,&value)){
			if (key && value){
				s = value;
				if (s->pos==i){
					printf("\t%s",key);
				}
			}
		}
	}
	printf("\n");
	
	return 1;
}



int main( int argc, char *argv[] )
{
	struct deltadb *db = deltadb_create();

	parse_input(db);

	deltadb_delete(db);

	return 0;
}
