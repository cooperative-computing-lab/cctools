/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "nvpair_database.h"
#include "nvpair_private.h"

#include "hash_table.h"
#include "debug.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

struct nvpair_database {
	struct hash_table *table;
	const char *logdir;
	int logyear;
	int logday;
	FILE *logfile;
	time_t last_log_time;
};

#define NVPAIR_LINE_MAX 4096

/* Take the current state of the table and write it out verbatim to a checkpoint file. */

static int checkpoint_write( struct nvpair_database *db, const char *filename )
{
	char *key;
	struct nvpair *nv;

	FILE *file = fopen(filename,"w");
	if(!file) return 0;

	hash_table_firstkey(db->table);
	while((hash_table_nextkey(db->table,&key,(void**)&nv))) {
		fprintf(file,"key %s\n",key);
		nvpair_print_text(nv,file);
	}

	fclose(file);

	return 1;
}

/* Get a complete checkpoint file and reconstitute the state of the table. */

static int checkpoint_read( struct nvpair_database *db, const char *filename )
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

/* Ensure that the history is writing to the correct log file for the current time. */

static void log_select( struct nvpair_database *db )
{
	time_t current = time(0);
	struct tm *t = gmtime(&current);
	int write_checkpoint_file = 0;

	// If the file is open to the right file, continue as before.
	if(db->logfile && (t->tm_year+1900)==db->logyear && t->tm_yday==db->logday) return;

	// If a log file is already open, close it.
	if(db->logfile) {
		fclose(db->logfile);
		write_checkpoint_file = 1;
	}

	db->logyear = t->tm_year + 1900;
	db->logday = t->tm_yday;

	// Ensure that we have a directory.
	char filename[NVPAIR_LINE_MAX];
	sprintf(filename,"%s/%d",db->logdir,db->logyear);
	mkdir(filename,0777);
	
	// Open the new file.
	sprintf(filename,"%s/%d/%d.log",db->logdir,db->logyear,db->logday);
	db->logfile = fopen(filename,"a");
	if(!db->logfile) fatal("could not open log file %s: %s",filename,strerror(errno));

	// If we switched from one log to another, write an intermediate checkpoint.
	if(write_checkpoint_file) {
		sprintf(filename,"%s/%d/%d.ckpt",db->logdir,db->logyear,db->logday);
		checkpoint_write(db,filename);
	}

}

/* If time has advanced since the last event, log a time record. */

static void log_time( struct nvpair_database *db )
{
	time_t current = time(0);	
	if(db->last_log_time!=current) {
		db->last_log_time = current;
		fprintf(db->logfile,"T %ld\n",(long)current);
	}
} 

/* Log an event indicating that an object was created, followed by the list of values. */

static void log_create( struct nvpair_database *db, const char *key, struct nvpair *nv )
{
	log_select(db);
	log_time(db);

	fprintf(db->logfile,"C %s\n",key);
	nvpair_print_text(nv,db->logfile);
}

/* Log update events that indicate the difference between objects a (old) and b (new)*/

static void log_updates( struct nvpair_database *db, const char *key, struct nvpair *a, struct nvpair *b )
{
	log_select(db);

	char *name, *avalue, *bvalue;

	// For each item in the old nvpair:
	// If the new one is different, log an update event.
	// If the new one is missing, log a remove event.

	hash_table_firstkey(a->table);
	while(hash_table_nextkey(a->table,&name,(void**)&avalue)) {

		// Do not log these special cases, because they do not carry new information:
		if(!strcmp(name,"lastheardfrom")) continue;
		if(!strcmp(name,"uptime")) continue;

		bvalue = hash_table_lookup(b->table,name);
		if(bvalue) {
			if(!strcmp(avalue,bvalue)) {
				// items match, do nothing.
			} else { 
				log_time(db);
				fprintf(db->logfile,"U %s %s %s\n",key,name,bvalue);
			}
		} else {
       			log_time(db);
			fprintf(db->logfile,"R %s %s\n",key,name);
		}
	}

	// For each item in the new nvpair:
	// If it doesn't exist in the old one, log an update event.

	hash_table_firstkey(b->table);
	while(hash_table_nextkey(b->table,&name,(void**)&bvalue)) {
		avalue = hash_table_lookup(a->table,name);
		if(!avalue) {
       			log_time(db);
			fprintf(db->logfile,"U %s %s %s\n",key,name,bvalue);
		}
	}
} 

/* Log an event indicating an entire object was deleted. */

static void log_delete( struct nvpair_database *db, const char *key )
{
	log_select(db);
	log_time(db);
	fprintf(db->logfile,"D %s\n",key);
}

/* Push any buffered output out to the log. */

static void log_flush( struct nvpair_database *db )
{
	if(db->logfile) fflush(db->logfile);
}

/*
Replay a given log file into the hash table, up to the given snapshot time.
Returns true if file could be open and played, false otherwise.
*/

static int log_replay( struct nvpair_database *db, const char *filename, time_t snapshot)
{
	FILE *file = fopen(filename,"r");
	if(!file) return 0;
	
	time_t current = 0;
	struct nvpair *nv;

	char line[NVPAIR_LINE_MAX];
	char key[NVPAIR_LINE_MAX];
	char name[NVPAIR_LINE_MAX];
	char value[NVPAIR_LINE_MAX];
	char oper;
	
	while(fgets(line,sizeof(line),file)) {

		int n = sscanf(line,"%c %s %s %[^\n]",&oper,key,name,value);
		if(n<1) continue;

		switch(oper) {
			case 'C':
				nv = nvpair_create();
				nvpair_parse_stream(nv,file);
				nvpair_delete(hash_table_remove(db->table,name));
				hash_table_insert(db->table,key,nv);
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
				current = atol(key);
				if(current>snapshot) break;
				break;
			default:
				debug(D_NOTICE,"corrupt log data in %s: %s",filename,line);
				break;
		}
	}
	
	fclose(file);

	return 1;
}

/*
Recover the state of the table by loading the appropriate checkpoint
file, then playing the corresponding log until the snapshot time is reached.
Returns true if successful, false if files could not be played.
*/

static int log_recover( struct nvpair_database *db, time_t snapshot )
{      
	char filename[NVPAIR_LINE_MAX];

	struct tm *t = gmtime(&snapshot);

	int year = t->tm_year + 1900;
	int day = t->tm_yday;

	sprintf(filename,"%s/%d/%d.ckpt",db->logdir,year,day);
	checkpoint_read(db,filename);

	sprintf(filename,"%s/%d/%d.log",db->logdir,year,day);
	log_replay(db,filename,snapshot);

	return 1;
}

struct nvpair_database * nvpair_database_create( const char *logdir )
{
	if(logdir) {
		int result = mkdir(logdir,0777);
		if(result<0 && errno!=EEXIST) return 0;
	}

	struct nvpair_database *db = malloc(sizeof(*db));
	db->table = hash_table_create(0,0);
	db->logyear = 0;
	db->logday = 0;
	db->logfile = 0;
	db->last_log_time = 0;
	db->logdir = 0;

	if(logdir) {
		db->logdir = strdup(logdir);
		log_recover(db,time(0));
	}

	return db;
} 

void nvpair_database_insert( struct nvpair_database *db, const char *key, struct nvpair *nv )
{
	struct nvpair *old = hash_table_remove(db->table,key);

	hash_table_insert(db->table,key,nv);

	if(db->logdir) {
		if(old) {
			log_updates(db,key,old,nv);
			nvpair_delete(old);
		} else { 
			log_create(db,key,nv);
		}
	}

	log_flush(db);
} 

struct nvpair * nvpair_database_lookup( struct nvpair_database *db, const char *key )
{
	return hash_table_lookup(db->table,key);
} 

struct nvpair * nvpair_database_remove( struct nvpair_database *db, const char *key )
{
	const char *nkey = strdup(key);

	struct nvpair *nv = hash_table_remove(db->table,nkey);
	if(db->logdir && nv) {
		log_delete(db,nkey);
		log_flush(db);
	}
	return nv;
} 

void nvpair_database_firstkey( struct nvpair_database *db )
{
	hash_table_firstkey(db->table);
}

int  nvpair_database_nextkey( struct nvpair_database *db, char **key, struct nvpair **nv )
{
	return hash_table_nextkey(db->table,key,(void**)nv);
}


