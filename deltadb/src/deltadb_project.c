/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "nvpair.h"
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
	char **attr_list;
	int attr_len;
};

struct deltadb * deltadb_create( )
{
	struct deltadb *db = malloc(sizeof(*db));
	db->table = hash_table_create(0,0);
	db->attr_list = NULL;
	db->attr_len = 0;
	return db;
}

void deltadb_delete( struct deltadb *db )
{
  // should delete all nvpairs in the table here
	if(db->table) hash_table_delete(db->table);
	//if(db->logfile) fclose(db->logfile);
	free(db);
}


/*
Replay a given log file into the hash table, up to the given snapshot time.
Return true if the stoptime was reached.
*/

#define NVPAIR_LINE_MAX 4096

static int log_play( struct deltadb *db )
{
	FILE *stream = stdin;
	time_t current = 0;
	struct nvpair *nv;
	int line_number = 0;

	char line[NVPAIR_LINE_MAX];
	char key[NVPAIR_LINE_MAX];
	char name[NVPAIR_LINE_MAX];
	char value[NVPAIR_LINE_MAX];
	char oper;

	int notime = 1;
	while(fgets(line,sizeof(line),stream)) {
		line_number += 1;

		if (line[0]=='\n') return 0;

		int n = sscanf(line,"%c %s %s %[^\n]",&oper,key,name,value);
		if(n<1) continue;

		int i,include;
		switch(oper) {
			case 'C':
				nv = nvpair_create();
				int num_pairs = nvpair_parse_stream_limited(nv,stream,db->attr_list,db->attr_len);
				if (num_pairs>0){

					if (notime){
						printf("T %lld\n",(long long)current);
						notime = 0;
					}
					printf("C %s\n",key);
					nvpair_print_text(nv,stdout);

				}
				nvpair_delete(nv);
				break;
			case 'D':
				if (notime){
					printf("T %lld\n",(long long)current);
					notime = 0;
				}
				printf("%s",line);
				break;
			case 'U':
				include = 0;
				for(i=0; i<db->attr_len; i++){
					if(strcmp(name,db->attr_list[i])==0){
						include = 1;
						break;
					}
				}
				if (include>0){
					if (notime){
						printf("T %lld\n",(long long)current);
						notime = 0;
					}
					printf("%s",line);
				}
				break;
			case 'R':
				include = 0;
				for(i=0; i<db->attr_len; i++){
					if(strcmp(name,db->attr_list[i])==0){
						include = 1;
						break;
					}
				}
				if (include>0){
					if (notime){
						printf("T %lld\n",(long long)current);
						notime = 0;
					}
					printf("%s",line);
				}
				break;
			case 'T':
				current = atol(key);
				notime = 1;
				break;
			default:
				debug(D_NOTICE,"corrupt log data[%i]: %s",line_number,line);
				fflush(stderr);
				break;
		}
	}
	return 0;
}

/*
Play the log from start_time to end_time by opening the appropriate
checkpoint file and working ahead in the various log files.
*/

static int parse_input( struct deltadb *db )
{
	while(1) {
		int keepgoing = log_play(db);
		if(!keepgoing) break;
	}
	return 1;
}

int main( int argc, char *argv[] )
{
	struct deltadb *db = deltadb_create();

	int i;
	db->attr_len = argc-1;
	db->attr_list = malloc(sizeof(char*)*db->attr_len);
	for (i=1; i<argc; i++){
		db->attr_list[i-1] = argv[i];
	}


	parse_input(db);

	deltadb_delete(db);

	return 0;
}
