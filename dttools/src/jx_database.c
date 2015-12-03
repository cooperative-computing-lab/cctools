/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

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

struct jx_database {
	struct hash_table *table;
	const char *logdir;
	int logyear;
	int logday;
	FILE *logfile;
	time_t last_log_time;
};

/* Take the current state of the table and write it out verbatim to a checkpoint file. */

static int checkpoint_write( struct jx_database *db, const char *filename )
{
	char *key;
	struct jx *jobject;

	FILE *file = fopen(filename,"w");
	if(!file) return 0;

	fprintf(file,"{\n");

	hash_table_firstkey(db->table);
	while((hash_table_nextkey(db->table,&key,(void**)&jobject))) {
		fprintf(file,"\"%s\":\n",key);
		jx_print_stream(jobject,file);
	}

	fprintf(file,"}\n");

	fclose(file);

	return 1;
}

/* Get a complete checkpoint file and reconstitute the state of the table. */

static int checkpoint_read( struct jx_database *db, const char *filename )
{
	FILE * file = fopen(filename,"r");
	if(!file) return 0;

	/* Load the entire checkpoint into one json object */
	struct jx *jcheckpoint = jx_parse_stream(file);

	fclose(file);

	jx_assert(jcheckpoint,JX_OBJECT);

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

/* Ensure that the history is writing to the correct log file for the current time. */

static void log_select( struct jx_database *db )
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
	char filename[PATH_MAX];
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

static void log_time( struct jx_database *db )
{
	time_t current = time(0);
	if(db->last_log_time!=current) {
		db->last_log_time = current;

		struct jx *j = jx_arrayv(
			jx_string("T"),
			jx_integer(current),
			0);

		jx_print_stream(j,db->logfile);
		jx_delete(j);
		fprintf(db->logfile,"\n");
	}
}

/* Log a complete json message with time, a newline, then delete it. */

static void log_message( struct jx_database *db, struct jx *j )
{
	log_select(db);
	log_time(db);
	jx_print_stream(j,db->logfile);
	fprintf(db->logfile,"\n");
	jx_delete(j);
}

/* Log an event indicating that an object was created, followed by object itself */

static void log_create( struct jx_database *db, const char *key, struct jx *j )
{
	log_message( db, jx_arrayv(jx_string("C"),jx_string(key),jx_copy(j),0) );
}

/* Log update events that indicate the difference between objects a (old) and b (new)*/

static void log_updates( struct jx_database *db, const char *key, struct jx *a, struct jx *b )
{
	// For each item in the old object:
	// If the new one is different, log an update event.
	// If the new one is missing, log a remove event.

	struct jx_pair *p;
	for(p=a->pairs;p;p=p->next) {

		const char *name = p->key->string_value;
		struct jx *avalue = p->value;

		// Do not log these special cases, because they do not carry new information:
		if(!strcmp(name,"lastheardfrom")) continue;
		if(!strcmp(name,"uptime")) continue;

		struct jx *bvalue = jx_lookup(b,name);
		if(bvalue) {
			if(jx_equals(avalue,bvalue)) {
				// items match, do nothing.
			} else {
				// item changed, print it.
				log_message(db,jx_arrayv(jx_string("U"),jx_string(key),jx_string(name),jx_copy(bvalue),0));
			}
		} else {
			// item was removed.
			log_message(db,jx_arrayv(jx_string("R"),jx_string(key),jx_string(name),0));
		}
	}

	// For each item in the new object:
	// If it doesn't exist in the old one, log an update event.

	for(p=b->pairs;p;p=p->next) {

		const char *name = p->key->string_value;
		struct jx *bvalue = p->value;

		struct jx *avalue = jx_lookup(a,name);
		if(!avalue) {
			log_message(db,jx_arrayv(jx_string("U"),jx_string(key),jx_string(name),jx_copy(bvalue)));
		}
	}
}

/* Log an event indicating an entire object was deleted. */

static void log_delete( struct jx_database *db, const char *key )
{
	log_message(db,jx_arrayv(jx_string("D"),jx_string(key),0));
}

/* Push any buffered output out to the log. */

static void log_flush( struct jx_database *db )
{
	if(db->logfile) fflush(db->logfile);
}

/*
Replay a log record like [ "C", key, value ]
*/

static int log_replay_create( struct jx_database *db, struct jx_item *items )
{
	if(!items || !items->value || items->value->type!=JX_STRING) return 0;

	const char *key = items->value->string_value;

	items = items->next;

	if(!items || !items->value) return 0;

	/*
	Special case: move the record from the log entry
	to the db in order to avoid excessive copying.
	jx_delete doesn't mind null entries.
	*/

	hash_table_insert(db->table,key,items->value);
	items->value = 0;	

	return 1;
}

/*
Replay a log record like [ "D", key ]
*/

static int log_replay_delete( struct jx_database *db, struct jx_item *items )
{
	if(!items || !items->value || items->value->type!=JX_STRING) return 0;

	const char *key = items->value->string_value;

	jx_delete(hash_table_remove(db->table,key));

	return 1;
}

/*
Replay a log record like [ "U", key, name, value ]
*/

static int log_replay_update( struct jx_database *db, struct jx_item *items )
{
	if(!items || !items->value || items->value->type!=JX_STRING) return 0;
	const char *key = items->value->string_value;

	items = items->next;

	if(!items || !items->value || items->value->type!=JX_STRING) return 0;
	struct jx *name = items->value;

	items = items->next;

	if(!items || !items->value) return 0;
	struct jx *value = items->value;

	struct jx *record = hash_table_lookup(db->table,key);
	if(record) {
		jx_delete(jx_remove(record,name));
		jx_insert(record,jx_copy(name),value);
	}

	return 1;
}

/*
Replay a log record like [ "R", key, name ]
*/

static int log_replay_remove( struct jx_database *db, struct jx_item *items )
{
	if(!items || !items->value || items->value->type!=JX_STRING) return 0;
	const char *key = items->value->string_value;

	items = items->next;

	if(!items || !items->value || items->value->type!=JX_STRING) return 0;
	struct jx *name = items->value;

	struct jx *record = hash_table_lookup(db->table,key);
	if(record) {
		jx_delete(jx_remove(record,name));
	}

	return 1;
}

/* Replay a log record like [ "T", time ] for a time update. */

static int log_replay_time( struct jx_database *db, struct jx_item *items, time_t *current )
{
	if(!items || !items->value || items->value->type!=JX_INTEGER) return 0;
	*current = items->value->integer_value;
	return 1;
}

/* Report an invalid bit of data in the log. */

static void corrupt_data( const char *filename, struct jx *j )
{
	char *str = jx_print_string(j);
	debug(D_NOTICE,"corrupt data in log %s: %s\n",filename,str);
	free(str);
}

/*
Replay a given log file into the hash table, up to the given snapshot time.
Returns true if file could be open and played, false otherwise.
*/

static int log_replay( struct jx_database *db, const char *filename, time_t snapshot)
{
	time_t current = 0;

	FILE *file = fopen(filename,"r");
	if(!file) return 0;

	struct jx_parser *parser = jx_parser_create();
	jx_parser_read_stream(parser,file);

	while(1) {
		struct jx *logentry = jx_parse(parser);
		if(!logentry) break;

		if(!jx_istype(logentry,JX_ARRAY)) {
			corrupt_data(filename,logentry);
			continue;
		}

		struct jx_item *item = logentry->items;
		if(!item || !jx_istype(item->value,JX_STRING)) {
			corrupt_data(filename,logentry);
			continue;
		}

		int result = 0;

		char op = item->value->string_value[0];
		if(op=='C') {
			result = log_replay_create(db,item->next);
		} else if(op=='D') {
			result = log_replay_delete(db,item->next);
		} else if(op=='U') {
			result = log_replay_update(db,item->next);
		} else if(op=='R') {
			result = log_replay_remove(db,item->next);
		} else if(op=='T') {
			result = log_replay_time(db,item->next,&current);
			if(current>snapshot) break;
		} else {
			// invalid operation
			result = 0;
		}
		if(!result) {
			corrupt_data(filename,logentry);
		}
	}

	jx_parser_delete(parser);
	fclose(file);
	return 1;
}

/*
Recover the state of the table by loading the appropriate checkpoint
file, then playing the corresponding log until the snapshot time is reached.
Returns true if successful, false if files could not be played.
*/

static int log_recover( struct jx_database *db, time_t snapshot )
{
	char filename[PATH_MAX];

	struct tm *t = gmtime(&snapshot);

	int year = t->tm_year + 1900;
	int day = t->tm_yday;

	sprintf(filename,"%s/%d/%d.ckpt",db->logdir,year,day);
	checkpoint_read(db,filename);

	sprintf(filename,"%s/%d/%d.log",db->logdir,year,day);
	log_replay(db,filename,snapshot);

	return 1;
}

struct jx_database * jx_database_create( const char *logdir )
{
	if(logdir) {
		int result = mkdir(logdir,0777);
		if(result<0 && errno!=EEXIST) return 0;
	}

	struct jx_database *db = malloc(sizeof(*db));
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

void jx_database_insert( struct jx_database *db, const char *key, struct jx *nv )
{
	struct jx *old = hash_table_remove(db->table,key);

	hash_table_insert(db->table,key,nv);

	if(db->logdir) {
		if(old) {
			log_updates(db,key,old,nv);
		} else {
			log_create(db,key,nv);
		}
	}

	if(old) jx_delete(old);

	log_flush(db);
}

struct jx * jx_database_lookup( struct jx_database *db, const char *key )
{
	return hash_table_lookup(db->table,key);
}

struct jx * jx_database_remove( struct jx_database *db, const char *key )
{
	const char *nkey = strdup(key);

	struct jx *j = hash_table_remove(db->table,key);
	if(db->logdir && j) {
		log_delete(db,nkey);
		log_flush(db);
	}
	return j;
}

void jx_database_firstkey( struct jx_database *db )
{
	hash_table_firstkey(db->table);
}

int  jx_database_nextkey( struct jx_database *db, char **key, struct jx **j )
{
	return hash_table_nextkey(db->table,key,(void**)j);
}

/* vim: set noexpandtab tabstop=4: */
