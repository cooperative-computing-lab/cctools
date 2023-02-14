/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb.h"
#include "jx_print.h"
#include "jx_parse.h"

#include "hash_table.h"
#include "debug.h"
#include "nvpair.h"
#include "nvpair_jx.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdarg.h>

struct deltadb {
	struct hash_table *table;
	const char *logdir;
	int logyear;
	int logday;
	FILE *logfile;
	time_t last_log_time;
	bool snapshot;
};

/* Take the current state of the table and write it out verbatim to a checkpoint file. */

static int checkpoint_write( struct deltadb *db, const char *filename )
{
	char *key;
	struct jx *jobject;
	int first = 1;

	FILE *file = fopen(filename,"w");
	if(!file) return 0;

	fprintf(file,"{\n");

	hash_table_firstkey(db->table);
	while((hash_table_nextkey(db->table,&key,(void**)&jobject))) {
		if(!first) {
			fprintf(file,",\n");
		} else {
			first = 0;
		}
		fprintf(file,"\"%s\":\n",key);
		jx_print_stream(jobject,file);
	}

	fprintf(file,"}\n");

	fclose(file);

	return 1;
}

/*
Read a checkpoint in the (deprecated) nvpair format.  This will allow for a seamless upgrade by permitting the new JX database to continue from an nvpair checkpoint.
*/

static int compat_checkpoint_read( struct deltadb *db, const char *filename )
{
	FILE * file = fopen(filename,"r");
	if(!file) return 0;

	while(1) {
		struct nvpair *nv = nvpair_create();
		if(nvpair_parse_stream(nv,file)) {
			const char *key = nvpair_lookup_string(nv,"key");
			if(key) {
				nvpair_delete(hash_table_remove(db->table,key));
				struct jx *j = nvpair_to_jx(nv);
				hash_table_insert(db->table,key,j);
			}
			nvpair_delete(nv);
		} else {
			nvpair_delete(nv);
			break;
		}
	}

	fclose(file);

	return 1;
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
		debug(D_NOTICE, "could not parse checkpoint file, falling back to compatibility mode");
		jx_delete(jcheckpoint);
		return compat_checkpoint_read(db,filename);
	}

	/* For each key and value, move the value over to the hash table. */

	struct jx_pair *p;
	for(p=jcheckpoint->u.pairs;p;p=p->next) {
		if(p->key->type!=JX_STRING) continue;
		hash_table_insert(db->table,p->key->u.string_value,p->value);
		p->value = 0;
	}

	/* Delete the leftover object with empty pairs. */

	jx_delete(jcheckpoint);

	return 1;
}

/* Ensure that the history is writing to the correct log file for the current time. */

static void log_select( struct deltadb *db )
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

	// Reset the time so that an absolute time record comes next.
	db->last_log_time = 0;

}

/* If time has advanced since the last event, log a time record. */

static void log_time( struct deltadb *db )
{
	time_t current = time(0);
	if(db->last_log_time==0) {
		fprintf(db->logfile,"T %lld\n",(long long)current);
		db->last_log_time = current;
	} else if(db->last_log_time!=current) {
		fprintf(db->logfile,"t %lld\n",(long long)current-db->last_log_time);
		db->last_log_time = current;
	}

}

/* Log a complete message with time, a newline, then delete it. */

static void log_message( struct deltadb *db, const char *fmt, ... )
{
	va_list args;
	va_start(args,fmt);

	log_select(db);
	log_time(db);
	vfprintf(db->logfile,fmt,args);

	va_end(args);
}

/* Log an event indicating that an object was created, followed by object itself */

static void log_create( struct deltadb *db, const char *key, struct jx *j )
{
	char *str = jx_print_string(j);
	log_message(db,"C %s %s\n",key,str);
	free(str);
}

/* Log update events that indicate the difference between objects a (old) and b (new)*/

static void log_updates( struct deltadb *db, const char *key, struct jx *a, struct jx *b )
{
	// u is the object containing the update
	struct jx *u = jx_object(0);

	// For each item in the old object:
	// If the new item is different, add it to an update object.
	// If the new item is missing, log a remove event.

	struct jx_pair *p;
	for(p=a->u.pairs;p;p=p->next) {

		const char *name = p->key->u.string_value;
		struct jx *avalue = p->value;

		// Do not log these special cases, because they do not carry new information:
		if(!strcmp(name,"lastheardfrom")) continue;
		if(!strcmp(name,"uptime")) continue;

		struct jx *bvalue = jx_lookup(b,name);
		if(bvalue) {
			if(jx_equals(avalue,bvalue)) {
				// items match, do nothing.
			} else {
				// item changed, include it in the update object.
				jx_insert(u,jx_string(name),jx_copy(bvalue));
			}
		} else {
			// item was removed, log a remove record instead
			log_message(db,"R %s %s\n",key,name);
		}
	}

	// For each item in the new object:
	// If it doesn't exist in the old one, add it to the update object.

	for(p=b->u.pairs;p;p=p->next) {

		const char *name = p->key->u.string_value;
		struct jx *bvalue = p->value;

		struct jx *avalue = jx_lookup(a,name);
		if(!avalue) {
			// item changed, include it.
			jx_insert(u,jx_string(name),jx_copy(bvalue));
		}
	}

	// If the update is not empty, log it as a merge (M) event.
	if(u->u.pairs) {
		char *str = jx_print_string(u);
		log_message(db,"M %s %s\n",key,str);
		free(str);
	}

	jx_delete(u);
}

/* Log an event indicating an entire object was deleted. */

static void log_delete( struct deltadb *db, const char *key )
{
	log_message(db,"D %s\n",key);
}

/* Push any buffered output out to the log. */

static void log_flush( struct deltadb *db )
{
	if(db->logfile) fflush(db->logfile);
}

/* Report an invalid bit of data in the log. */

static void corrupt_data( const char *filename, const char *line )
{
	debug(D_NOTICE,"corrupt data in %s: %s\n",filename,line);

}

/* Accept an update object and transfer its fields to a current item. */
/* Note that if the current key does not exist, the update becomes the new value. */

static void handle_merge( struct deltadb *db, const char *key, struct jx *update )
{
	struct jx *current = hash_table_remove(db->table,key);
	struct jx *merged = jx_merge(current, update, NULL);

	hash_table_insert(db->table,key,merged);

	jx_delete(update);
	jx_delete(current);
}

/*
Replay a given log file into the hash table, up to the given snapshot time.
Returns true if file could be open and played, false otherwise.
*/

#define LOG_LINE_MAX 65536

static int log_replay( struct deltadb *db, const char *filename, time_t snapshot)
{
	char whole_line[LOG_LINE_MAX];
	char value[LOG_LINE_MAX];
	char name[LOG_LINE_MAX];
	char key[LOG_LINE_MAX];
	int n;
	struct jx *jvalue, *jobject;

	long long current = 0;

	FILE *file = fopen(filename,"r");
	if(!file) return 0;

	while(fgets(whole_line,sizeof(whole_line),file)) {
		char *line = whole_line;

		reconsider:

		if(line[0]=='C') {
			n = sscanf(line,"C %s %[^\n]",key,value);
			if(n==1) {
				struct nvpair *nv = nvpair_create();
				nvpair_parse_stream(nv,file);
				jvalue = nvpair_to_jx(nv);
				hash_table_insert(db->table,key,jvalue);
			} else if(n==2) {
				jvalue = jx_parse_string(value);
				if(jvalue) {
					hash_table_insert(db->table,key,jvalue);
				} else {
					corrupt_data(filename,line);
				}
			} else {
				corrupt_data(filename,line);
				continue;
			}
		} else if(line[0]=='M') {
			n = sscanf(line,"M %s %[^\n]",key,value);
			if(n==2) {
				jvalue = jx_parse_string(value);
				if(jvalue) {
					handle_merge(db,key,jvalue);
				} else {
					corrupt_data(filename,line);
				}
			} else {
				corrupt_data(filename,line);
				continue;
			}
		} else if(line[0]=='D') {
			n = sscanf(line,"D %s\n",key);
			if(n!=1) {
				corrupt_data(filename,line);
				continue;
			}
			jx_delete(hash_table_remove(db->table,key));
		} else if(line[0]=='U') {
			n=sscanf(line,"U %s %s %[^\n],",key,name,value);
			if(n!=3) {
				corrupt_data(filename,line);
				continue;
			}
			jobject = hash_table_lookup(db->table,key);
			if(!jobject) {
				corrupt_data(filename,line);
				continue;
			}
			jvalue = jx_parse_string(value);
			if(!jvalue) jvalue = jx_string(value);

			struct jx *jname = jx_string(name);
			jx_delete(jx_remove(jobject,jname));
			jx_insert(jobject,jname,jvalue);
		} else if(line[0]=='R') {
			n=sscanf(line,"R %s %s %s",key,name,value);
			if(n==3) {
				/*
				A third field here indicates a corrupted record.
				Check to see if the final character of key is a
				valid record type.
				*/
				char type = name[strlen(name)-1];

				if(strchr("CDUMRTt",type)) {
					/* If so, remove the character and process the R record */
					name[strlen(name)-1] = 0;

					jobject = hash_table_lookup(db->table,key);
					if(!jobject) {
						corrupt_data(filename,line);
						continue;
					}
					struct jx *jname = jx_string(name);
					jx_delete(jx_remove(jobject,jname));
					jx_delete(jname);

					name[strlen(name)] = type;

					/* Now process the remainder of the line as a new command. */
					int position = strlen(key) + strlen(name) + 2;
					line = &line[position];
					goto reconsider;
				} else {
					/* Invalid type: the line is totally corrupted. */
					corrupt_data(filename,line);
					continue;
				}

			} else if(n==2) {
				/* This is a correct record with just a key and value */
				jobject = hash_table_lookup(db->table,key);
				if(!jobject) {
					corrupt_data(filename,line);
					continue;
				}
				struct jx *jname = jx_string(name);
				jx_delete(jx_remove(jobject,jname));
				jx_delete(jname);
			} else {
				corrupt_data(filename,line);
				continue;
			}
		} else if(line[0]=='T') {
			n = sscanf(line,"T %lld",&current);
			if(n!=1) {
				corrupt_data(filename,line);
				continue;
			}
			if(current>snapshot) break;
		} else if(line[0]=='t') {
			long long change;
			n = sscanf(line,"t %lld",&change);
			if(n!=1) {
				corrupt_data(filename,line);
				continue;
			}
			current = current + change;
			if(current>snapshot) break;
		} else if(line[0]=='\n') {
			continue;
		} else {
			corrupt_data(filename,line);
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

static int log_recover( struct deltadb *db, time_t snapshot )
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

static struct deltadb * deltadb_create_instance( const char *logdir, time_t timestamp, bool snapshot)
{
	if(logdir) {
		int result = mkdir(logdir,0777);
		if(result<0 && errno!=EEXIST) return 0;
	}

	struct deltadb *db = malloc(sizeof(*db));
	db->table = hash_table_create(0,0);
	db->logyear = 0;
	db->logday = 0;
	db->logfile = 0;
	db->last_log_time = 0;
	db->logdir = 0;
	db->snapshot = snapshot;

	if(logdir) {
		db->logdir = strdup(logdir);
		log_recover(db,timestamp);
	}

	return db;
}

struct deltadb * deltadb_create( const char *logdir )
{
	return deltadb_create_instance(logdir, time(0), false);
}

struct deltadb * deltadb_create_snapshot( const char *logdir, time_t timestamp )
{
	return deltadb_create_instance(logdir, timestamp, true);
}

void deltadb_insert( struct deltadb *db, const char *key, struct jx *nv )
{
	if (db->snapshot) {
		debug(D_ERROR, "can't modify a deltadb snapshot");
		return;
	}

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

struct jx * deltadb_lookup( struct deltadb *db, const char *key )
{
	return hash_table_lookup(db->table,key);
}

struct jx * deltadb_remove( struct deltadb *db, const char *key )
{
	if (db->snapshot) {
		debug(D_ERROR, "can't modify a deltadb snapshot");
		return 0;
	}

	const char *nkey = strdup(key);

	struct jx *j = hash_table_remove(db->table,key);
	if(db->logdir && j) {
		log_delete(db,nkey);
		log_flush(db);
	}
	return j;
}

void deltadb_firstkey( struct deltadb *db )
{
	hash_table_firstkey(db->table);
}

int  deltadb_nextkey( struct deltadb *db, char **key, struct jx **j )
{
	return hash_table_nextkey(db->table,key,(void**)j);
}

/* vim: set noexpandtab tabstop=4: */
