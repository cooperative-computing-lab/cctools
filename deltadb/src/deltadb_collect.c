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

static int log_play( struct hash_table *table, FILE *stream, const char *filename, time_t start_time, time_t end_time, int started)
{
	time_t current = 0;
	time_t last_ts = 0;
	struct nvpair *nv;

	char line[NVPAIR_LINE_MAX];
	char key[NVPAIR_LINE_MAX];
	char name[NVPAIR_LINE_MAX];
	char value[NVPAIR_LINE_MAX];
	char oper;

	int updates = 0;
	int creating = 0;
	int bad = 0;

	printf("T %i\n",(int)start_time);

	while(fgets(line,sizeof(line),stream)) {
		int n = 0;
		if (!creating){
			n = sscanf(line,"%c %s %s %[^\n]",&oper,key,name,value);
			switch(oper) {
				case 'C':
					if (n!=2) bad = 1;
					else if (started) creating = 1;
					break;
				case 'D':
					if (n!=2) bad = 1;
					break;
				case 'U':
					if (n!=4) bad = 1;
					break;
				case 'R':
					if (n!=3) bad = 1;
					break;
				case 'T':
					if (n!=2) bad = 1;
					break;
				default:
					if(!creating && started) bad = 1;
			}

		} else {
			n = sscanf(line,"%s %[^\n]",name,value);
			oper = '\0';
			if (n<=0) creating = 0;
			else if (n==1) bad = 1;
		}
		if (bad){
			debug(D_NOTICE,"corrupt log data: %s",line);
			bad = 0;
			continue;
		}


		if (started==1){
			//printf("(%s",line);
			if (line[0]=='T' && line[1]!=' ') // Handles corrupt times that cause the select to think it is done.
				continue;
			if (oper=='T'){
				last_ts = current;
				current = atol(key);
				if ((current-last_ts)>(24*3600) && last_ts>0)
					continue;
				if(current>end_time){
					return 0;
				}
			}
			printf("%s",line);
			continue;
		}
		if(n<1) continue;
		//printf("-\n");

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
				updates += 1;
				//printf("%d..%s\n",updates,key);
				nv = hash_table_lookup(table,key);
				if(nv) nvpair_insert_string(nv,name,value);
				break;
			case 'R':
				nv = hash_table_lookup(table,key);
				if(nv) nvpair_remove(nv,name);
				else debug(D_DEBUG,"no attribute to remove: %s",line);
				break;
			case 'T':
				last_ts = current;
				current = atol(key);
				if ((current-last_ts)>(24*3600) && last_ts>0)
					current = last_ts;
				if(started==0 && current>=start_time){
					hash_table_firstkey(table);
					char *object_key;

					while(hash_table_nextkey(table, &object_key, (void **)&nv)) {

						printf("C %s\n",nvpair_lookup_string(nv,"key"));
						nvpair_print_text(nv,stdout);
					}
					//printf(".Checkpoint End.\n");
					printf(line);
					started = 1;
				} else if(current>end_time) {
					return 0;
				} else if (started==0){
					//Lines before the start_time are skipped.
				}
				break;
			default:
				debug(D_NOTICE,"corrupt log data: %s",line);
				//printf("corrupt log data: %s",line);
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
	int file_errors = 0;

	struct tm *t = gmtime(&start_time);

	int year = t->tm_year + 1900;
	int day = t->tm_yday;

	sprintf(filename,"%s/%d/%d.ckpt",db->logdir,year,day);
	debug(D_DEBUG,"Reading file: %s",filename);
	checkpoint_read(db,filename);

	int started = 0;
	while(1) {
		sprintf(filename,"%s/%d/%d.log",db->logdir,year,day);
		FILE *file = fopen(filename,"r");
		debug(D_DEBUG,"Reading file: %s",filename);
		fprintf(stderr,"Reading file: %s\n",filename);
		fflush(stderr);
		if(!file) {
			file_errors += 1;
			debug(D_DEBUG,"couldn't open %s: %s",filename,strerror(errno));
			if (file_errors>5)
				break;
		} else {
			int keepgoing = log_play(db->table,file,filename,start_time,end_time,started);
			started = 1;

			fclose(file);

			if(!keepgoing) break;
		}

		day++;
		if(day>=365) {
			year++;
			day = 0;
		}
	}
	//printf(".Log End.\n");
	return 1;
}

int main( int argc, char *argv[] )
{
	struct tm t1, t2;

	memset(&t1,0,sizeof(t1));
	memset(&t2,0,sizeof(t2));



	int start_year, start_month, start_day, start_hour, start_minute, start_second;
	sscanf(argv[2], "%d-%d-%d@%d:%d:%d", &start_year, &start_month, &start_day, &start_hour, &start_minute, &start_second);
	if (start_hour>23)
		start_hour = 0;
	if (start_minute>23)
		start_minute = 0;
	if (start_second>23)
		start_second = 0;
	//printf("%d-%d-%d@%d:%d:%d",start_year, start_month, start_day, start_hour, start_minute, start_second);

	t1.tm_year = start_year-1900;
	t1.tm_mon = start_month-1;
	t1.tm_mday = start_day;
	t1.tm_hour = start_hour;
	t1.tm_min = start_minute;
	t1.tm_sec = start_second;


	time_t start_time = mktime(&t1);
	time_t stop_time;

	if (strlen(argv[3])>=14){
		sscanf(argv[3], "%d-%d-%d@%d:%d:%d", &start_year, &start_month, &start_day, &start_hour, &start_minute, &start_second);

		t2.tm_year = start_year-1900;
		t2.tm_mon = start_month-1;
		t2.tm_mday = start_day;
		t2.tm_hour = start_hour;
		t2.tm_min = start_minute;
		t2.tm_sec = start_second;

		stop_time = mktime(&t2);

	} else {
		int duration_value;
		char duration_metric;
		sscanf(argv[3], "%c%i", &duration_metric, &duration_value);

		t2.tm_year = start_year-1900;
		t2.tm_mon = start_month-1;
		t2.tm_mday = start_day;
		t2.tm_hour = start_hour;
		t2.tm_min = start_minute;
		t2.tm_sec = start_second;

		stop_time = mktime(&t2);
		if (duration_metric=='y')
			stop_time += duration_value*365*24*3600;
		else if (duration_metric=='w')
			stop_time += duration_value*7*24*3600;
		else if (duration_metric=='d')
			stop_time += duration_value*24*3600;
		else if (duration_metric=='h')
			stop_time += duration_value*3600;
		else if (duration_metric=='m')
			stop_time += duration_value*60;
		else if (duration_metric=='s')
			stop_time += duration_value;
	}



	//Time Zone thing to be fixed later
	start_time -= 5*3600;
	stop_time -= 5*3600;

	struct deltadb *db = deltadb_create(argv[1]);

	log_play_time(db,start_time,stop_time);

	deltadb_delete(db);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
