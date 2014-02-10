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
	const char *logdir;
	FILE *logfile;
};

struct deltadb * deltadb_create( const char *logdir )
{
	struct deltadb *db = malloc(sizeof(*db));
	db->table = hash_table_create(0,0);
	db->logdir = logdir;
	db->logfile = 0;
	return db;
}

void deltadb_delete( struct deltadb *db )
{
  // should delete all nvpairs in the table here
	if(db->table) hash_table_delete(db->table);
	if(db->logfile) fclose(db->logfile);
	free(db);
}


#define NVPAIR_LINE_MAX 4096

static int checkpoint_read( struct deltadb *db, const char *filename )
{
	FILE * file = fopen(filename,"r");
	if(!file) return 0;

	while(1) {
		struct nvpair *nv = nvpair_create();
		if(nvpair_parse_stream(nv,file)) {
			const char *key = nvpair_lookup_string(nv,"key");
			if(key) {
				nvpair_delete(hash_table_remove(db->table,key));
				hash_table_insert(db->table,key,nv);
			} else {
				nvpair_delete(nv);
			}
		} else {
			nvpair_delete(nv);
			break;
		}
	}

	fclose(file);

	return 1;
}

/*
Replay a given log file into the hash table, up to the given snapshot time.
Return true if the stoptime was reached.
*/

static int log_play( struct hash_table *table, FILE *stream, const char *filename, time_t end_time )
{
	time_t current = 0;
	struct nvpair *nv;

	char line[NVPAIR_LINE_MAX];
	char key[NVPAIR_LINE_MAX];
	char name[NVPAIR_LINE_MAX];
	char value[NVPAIR_LINE_MAX];
	char oper;
	
	while(fgets(line,sizeof(line),stream)) {

		int n = sscanf(line,"%c %s %s %[^\n]",&oper,key,name,value);
		if(n<1) continue;

		switch(oper) {
			case 'C':
				nv = nvpair_create();
				nvpair_parse_stream(nv,stream);
				nvpair_delete(hash_table_remove(table,name));
				hash_table_insert(table,key,nv);
				break;
			case 'D':
				nv = hash_table_remove(table,key);
				if(nv) nvpair_delete(nv);
				break;
			case 'U':
				nv = hash_table_lookup(table,key);
				if(nv) nvpair_insert_string(nv,name,value);
				break;
			case 'R':
				nv = hash_table_lookup(table,key);
				if(nv) nvpair_remove(nv,name);
				break;
			case 'T':
				current = atol(key);
				if(current>end_time) return 0;
				break;
			default:
				debug(D_NOTICE,"corrupt log data: %s",line);
				break;
		}
	}

	return 1;
}

/*
Play the log from start_time to end_time by opening the appropriate
checkpoint file and working ahead in the various log files.
*/

static int log_play_time( struct deltadb *db, time_t start_time, time_t end_time )
{      
	char filename[NVPAIR_LINE_MAX];

	struct tm *t = gmtime(&start_time);

	int year = t->tm_year + 1900;
	int day = t->tm_yday;

	sprintf(filename,"%s/%d/%d.ckpt",db->logdir,year,day);
	checkpoint_read(db,filename);

	while(1) {
		sprintf(filename,"%s/%d/%d.log",db->logdir,year,day);
		FILE *file = fopen(filename,"r");
		if(!file) {
			debug(D_NOTICE,"couldn't open %s: %s",filename,strerror(errno));
			break;
		}
		int keepgoing = log_play(db->table,file,filename,end_time);

		fclose(file);

		if(!keepgoing) break;

		day++;
		if(day>365) {
			year++;
			day = 1;
		}
	}

	return 1;
}

int main( int argc, char *argv[] )
{
	struct tm t1, t2;

	memset(&t1,0,sizeof(t1));
	memset(&t2,0,sizeof(t2));

	// March 1st, 2013
	t1.tm_year = 113;
	t1.tm_mon = 2;
	t1.tm_mday = 1;

	// April 1st, 2013
	t2.tm_year = 113;
	t2.tm_mon = 3;
	t2.tm_mday = 1;
	
	time_t start_time = mktime(&t1);
	time_t stop_time = mktime(&t2);

	struct deltadb *db = deltadb_create(argv[1]);

	log_play_time(db,start_time,stop_time);

	deltadb_delete(db);

	return 0;
}
