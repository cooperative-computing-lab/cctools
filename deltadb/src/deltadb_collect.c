/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_stream.h"

#include "jx_database.h"
#include "jx_print.h"
#include "jx_parse.h"

#include "hash_table.h"
#include "debug.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdarg.h>
#include <ctype.h>

struct deltadb {
	struct hash_table *table;
	const char *logdir;
	FILE *logfile;
	int output_started;
};

struct deltadb * deltadb_create( const char *logdir )
{
	struct deltadb *db = malloc(sizeof(*db));
	memset(db,0,sizeof(*db));
	db->table = hash_table_create(0,0);
	db->logdir = logdir;
	db->logfile = 0;
	return db;
}

/* Get a complete checkpoint file and reconstitute the state of the table. */

static int checkpoint_read( struct deltadb *db, const char *filename )
{
	FILE * file = fopen(filename,"r");
	if(!file) return 0;

	/* Load the entire checkpoint into one json object */
	struct jx *jcheckpoint = jx_parse_stream(file);

	fclose(file);

	if(!jcheckpoint || jcheckpoint->type!=JX_OBJECT) {
		fprintf(stderr,"checkpoint %s is not a valid json document!\n",filename);
		jx_delete(jcheckpoint);
		return 0;
	}

	/* For each key and value, move the value over to the hash table. */

	struct jx_pair *p;
	for(p=jcheckpoint->pairs;p;p=p->next) {
		jx_assert(p->key,JX_STRING);
		hash_table_insert(db->table,p->key->string_value,p->value);
		p->value = 0;
	}

	/* Delete the leftover object with empty pairs. */

	jx_delete(jcheckpoint);

	return 1;
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
	struct jx * jobject = hash_table_lookup(db->table,key);
	if(!jobject) {
		fprintf(stderr,"warning: key %s does not exist in table\n",key);
		return 1;
	}

	struct jx *jname = jx_string(name);
	jx_delete(jx_remove(jobject,jname));
	jx_insert(jobject,jname,jvalue);
	return 1;
}

int deltadb_remove_event( struct deltadb *db, const char *key, const char *name )
{
	struct jx *jobject = hash_table_lookup(db->table,key);
	if(!jobject) {
		fprintf(stderr,"warning: key %s does not exist in table\n",key);
		return 0;
	}

	struct jx *jname = jx_string(name);
	jx_delete(jx_remove(jobject,jname));
	jx_delete(jname);
	return 1;
}

int deltadb_time_event( struct deltadb *db, time_t starttime, time_t stoptime, time_t current )
{
	if(current>stoptime) return 0;

	if(current>starttime && !db->output_started) {
		printf("T %lld\n",(long long)current);
		char *key;
		struct jx *jvalue;
		hash_table_firstkey(db->table);
		while(hash_table_nextkey(db->table,&key,(void**)&jvalue)) {
			char *str = jx_print_string(jvalue);
			printf("C %s %s\n",key,str);
			free(str);
		}
		db->output_started = 1;
	}

	return 1;
}

int deltadb_post_event( struct deltadb *db, const char *line )
{
	if(db->output_started) {
		printf("%s",line);
	}
	return 1;
}

/*
Play the log from starttime to stoptime by opening the appropriate
checkpoint file and working ahead in the various log files.
*/

static int log_play_time( struct deltadb *db, time_t starttime, time_t stoptime )
{
	char filename[1024];
	int file_errors = 0;

	struct tm *starttm = localtime(&starttime);

	int year = starttm->tm_year + 1900;
	int day = starttm->tm_yday;

	struct tm *stoptm = localtime(&stoptime);

	int stopyear = stoptm->tm_year + 1900;
	int stopday = stoptm->tm_yday;

	sprintf(filename,"%s/%d/%d.ckpt",db->logdir,year,day);
	checkpoint_read(db,filename);

	while(1) {
		sprintf(filename,"%s/%d/%d.log",db->logdir,year,day);
		FILE *file = fopen(filename,"r");
		if(!file) {
			file_errors += 1;
			fprintf(stderr,"couldn't open %s: %s\n",filename,strerror(errno));
			if (file_errors>5)
				break;
		} else {
			int keepgoing = deltadb_process_stream(db,file,starttime,stoptime);
			starttime = 0;

			fclose(file);

			// If we reached the endtime in the file, stop.
			if(!keepgoing) break;
		}

		day++;
		if(day>=365) {
			year++;
			day = 0;
		}

		// If we have passed the file, stop.
		if(year>=stopyear && day>stopday) break;
	}

	return 1;
}

int suffix_to_multiplier( char suffix )
{
	switch(tolower(suffix)) {
	case 'y': return 60*60*24*365;
	case 'w': return 60*60*24*7;
	case 'd': return 60*60*24;
	case 'h': return 60*60;
	case 'm': return 60;
	default: return 1;
	}
}

time_t parse_time( const char *str, time_t current )
{
	struct tm t;
	int count;
	char suffix[2];
	int n;

	memset(&t,0,sizeof(t));

	if(!strcmp(str,"now")) {
		return current;
	}

	n = sscanf(str, "%d%[yYdDhHmMsS]", &count, suffix);
	if(n==2) {
		return current - count*suffix_to_multiplier(suffix[0]);
	}

	n = sscanf(str, "%d-%d-%d@%d:%d:%d", &t.tm_year,&t.tm_mon,&t.tm_mday,&t.tm_hour,&t.tm_min,&t.tm_sec);
	if(n==6) {
		if (t.tm_hour>23)
			t.tm_hour = 0;
		if (t.tm_min>23)
			t.tm_min = 0;
		if (t.tm_sec>23)
			t.tm_sec = 0;

		t.tm_year -= 1900;
		t.tm_mon -= 1;

		return mktime(&t);
	}

	n = sscanf(str, "%d-%d-%d", &t.tm_year,&t.tm_mon,&t.tm_mday);
	if(n==3) {
		t.tm_year -= 1900;
		t.tm_mon -= 1;

		return mktime(&t);
	}

	return 0;
}


int main( int argc, char *argv[] )
{
	if(argc!=4) {
		fprintf(stderr,"use: deltadb_collect <dbdir> <starttime> <stoptime>\n");
		fprintf(stderr,"Where times may be may be:\n");
		fprintf(stderr,"    YY-MM-DD@HH:MM:SS\n");
		fprintf(stderr,"    HH:MM:SS\n");
		fprintf(stderr,"    now\n");
		return 1;
	}

	time_t current = time(0);

	const char *dbdir = argv[1];
	time_t starttime = parse_time(argv[2],current);
	time_t stoptime = parse_time(argv[3],current);

	if(starttime==0) {
		fprintf(stderr,"invalid start time: %s\n",argv[2]);
		return 1;
	}

	if(stoptime==0) {
		fprintf(stderr,"invalid stop time:%s\n",argv[3]);
		return 1;
	}

	struct deltadb *db = deltadb_create(dbdir);

	log_play_time(db,starttime,stoptime);

	return 0;
}
