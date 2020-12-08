/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_stream.h"
#include "deltadb_reduction.h"
#include "deltadb_query.h"

#include "jx_eval.h"
#include "jx_database.h"
#include "jx_print.h"
#include "jx_parse.h"

#include "hash_table.h"
#include "debug.h"
#include "list.h"
#include "stringtools.h"
#include "nvpair.h"
#include "nvpair_jx.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdarg.h>
#include <ctype.h>

struct deltadb_query {
	struct hash_table *table;
	FILE *output_stream;
	int epoch_mode;
	struct jx *filter_expr;
	struct jx *where_expr;
	struct list * output_exprs;
	struct list * reduce_exprs;
	time_t display_every;
	time_t display_next;
	time_t deferred_time;
	deltadb_display_mode_t display_mode;
};

struct deltadb_query * deltadb_query_create()
{
	struct deltadb_query *db = malloc(sizeof(*db));
	memset(db,0,sizeof(*db));
	db->table = hash_table_create(0,0);
	db->output_stream = stdout;
	db->output_exprs = list_create();
	db->reduce_exprs = list_create();
	return db;
}

void deltadb_query_set_output( struct deltadb_query *db, FILE *stream )
{
	db->output_stream = stream;
}

void deltadb_query_set_display( struct deltadb_query *db, deltadb_display_mode_t mode )
{
	db->display_mode = mode;
}

void deltadb_query_set_filter( struct deltadb_query *db, struct jx *expr )
{
	db->filter_expr = expr;
}

void deltadb_query_set_where( struct deltadb_query *db, struct jx *expr )
{
	db->where_expr = expr;
}

void deltadb_query_set_epoch_mode( struct deltadb_query *db, int mode )
{
	db->epoch_mode = mode;
}

void deltadb_query_set_interval( struct deltadb_query *db, int interval )
{
	db->display_every = interval;
}

void deltadb_query_add_output( struct deltadb_query *db, struct jx *expr )
{
	list_push_tail(db->output_exprs,expr);
}

void deltadb_query_add_reduction( struct deltadb_query *db, struct deltadb_reduction *r )
{
	list_push_tail(db->reduce_exprs,r);
}

static int deltadb_boolean_expr( struct jx *expr, struct jx *data )
{
	if(!expr) return 1;

	struct jx *j = jx_eval(expr,data);
	int result = j && !jx_istype(j, JX_ERROR) && j->type==JX_BOOLEAN && j->u.boolean_value;
	jx_delete(j);
	return result;
}

/*
Read a checkpoint in the (deprecated) nvpair format.  This will allow for a seamless upgrade by permitting the new JX database to continue from an nvpair checkpoint.
*/

static int compat_checkpoint_read( struct deltadb_query *db, const char *filename )
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
				/* skip objects that don't match the filter */
				if(deltadb_boolean_expr(db->filter_expr,j)) {
					hash_table_insert(db->table,key,j);
				} else {
					jx_delete(j);
				}
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

static int checkpoint_read( struct deltadb_query *db, const char *filename )
{
	FILE * file = fopen(filename,"r");
	if(!file) return 0;

	/* Load the entire checkpoint into one json object */
	struct jx *jcheckpoint = jx_parse_stream(file);

	fclose(file);

	if(!jcheckpoint || jcheckpoint->type!=JX_OBJECT) {
		jx_delete(jcheckpoint);
		return compat_checkpoint_read(db,filename);
	}

	/* For each key and value, move the value over to the hash table. */

	/* Skip objects that don't match the filter. */

	struct jx_pair *p;
	for(p=jcheckpoint->u.pairs;p;p=p->next) {
		if(p->key->type!=JX_STRING) continue;
		if(!deltadb_boolean_expr(db->filter_expr,p->value)) continue;
		hash_table_insert(db->table,p->key->u.string_value,p->value);
		p->value = 0;
	}

	/* Delete the leftover object with empty pairs. */

	jx_delete(jcheckpoint);

	return 1;
}

static void display_reduce_exprs( struct deltadb_query *db, time_t current )
{
	/* Reset all reductions. */
	list_first_item(db->reduce_exprs);
	for(struct deltadb_reduction *r; (r = list_next_item(db->reduce_exprs));) {
		deltadb_reduction_reset(r);
	}

	/* For each item in the hash table: */

	char *key;
	struct jx *jobject;
	hash_table_firstkey(db->table);
	while(hash_table_nextkey(db->table,&key,(void**)&jobject)) {

		/* Skip if the where expression doesn't match */
		if(!deltadb_boolean_expr(db->where_expr,jobject)) continue;

		/* Update each reduction with its value. */
		list_first_item(db->reduce_exprs);
		for(struct deltadb_reduction *r; (r = list_next_item(db->reduce_exprs));) {
			struct jx *value = jx_eval(r->expr,jobject);
			if(value && !jx_istype(value, JX_ERROR)) {
				if(value->type==JX_INTEGER) {
					deltadb_reduction_update(r,(double)value->u.integer_value);
				} else if(value->type==JX_DOUBLE) {
					deltadb_reduction_update(r,value->u.double_value);
				} else {
					// treat non-numerics as 1, to facilitate operations like COUNT
					deltadb_reduction_update(r,1);
				}

				jx_delete(value);
			}
		}
	}

	/* Emit the current time */

	if(db->epoch_mode) {
		fprintf(db->output_stream,"%lld\t",(long long) current);
	} else {
		char str[32];
		strftime(str,sizeof(str),"%F %T",localtime(&current));
		fprintf(db->output_stream,"%s\t",str);
	}

	/* For each reduction, display the final value. */
	list_first_item(db->reduce_exprs);
	for(struct deltadb_reduction *r; (r = list_next_item(db->reduce_exprs));) {
		fprintf(db->output_stream,"%lf\t",deltadb_reduction_value(r));
	}

	fprintf(db->output_stream,"\n");

}

static void display_output_exprs( struct deltadb_query *db, time_t current )
{
	/* For each item in the table... */

	char *key;
	struct jx *jobject;
	hash_table_firstkey(db->table);
	while(hash_table_nextkey(db->table,&key,(void**)&jobject)) {

		/* Skip if the where expression doesn't match */

		if(!deltadb_boolean_expr(db->where_expr,jobject)) continue;

		/* Emit the current time */

		if(db->epoch_mode) {
			fprintf(db->output_stream,"%lld\t",(long long) current);
		} else {
			char str[32];
			strftime(str,sizeof(str),"%F %T",localtime(&current));
			fprintf(db->output_stream,"%s\t",str);
		}

		/* For each output expression, compute the value and print. */

		list_first_item(db->output_exprs);
		for(struct jx *j; (j = list_next_item(db->output_exprs));) {
			struct jx *jvalue = jx_eval(j,jobject);
			jx_print_stream(jvalue,db->output_stream);
			fprintf(db->output_stream,"\t");
			jx_delete(jvalue);
		}

		fprintf(db->output_stream,"\n");
	}
}

/*
To eliminate unnecessary T record on the output in streaming mode,
we store incoming T records as "deferred time" and then only
output if another record type intervenes.
*/

static void display_deferred_time( struct deltadb_query *db )
{
	if(db->deferred_time) {
		fprintf(db->output_stream,"T %ld\n",db->deferred_time);
		db->deferred_time = 0;
	}
}

int deltadb_create_event( struct deltadb_query *db, const char *key, struct jx *jobject )
{
	if(!deltadb_boolean_expr(db->filter_expr,jobject)) {
		jx_delete(jobject);
		return 1;
	}

	hash_table_insert(db->table,key,jobject);

	if(db->display_mode==DELTADB_DISPLAY_STREAM) {
		display_deferred_time(db);
		fprintf(db->output_stream,"C %s ",key);
		jx_print_stream(jobject,db->output_stream);
		fprintf(db->output_stream,"\n");
	}
	return 1;
}

int deltadb_delete_event( struct deltadb_query *db, const char *key )
{
	struct jx *jobject = hash_table_remove(db->table,key);

	if(jobject) {
		jx_delete(jobject);

		if(db->display_mode==DELTADB_DISPLAY_STREAM) {
			display_deferred_time(db);
			fprintf(db->output_stream,"D %s\n",key);
		}
	}
	return 1;
}

/*
Merge all the values of the update object into the current object,
replacing where they exist.  We previously used jx_merge here,
but the O(n^2) nature of the function and the heavy reliance on
malloc/free resulted in poor performance.  This function avoids
many malloc/frees by popping pairs off of the update in order,
finding matches in current, if needed, and then pushing the pair
on to the head of the current list.
*/

static void jx_merge_into( struct jx *current, struct jx *update )
{
	while(1) {
		struct jx_pair *p = update->u.pairs;
		if(!p) break;

		update->u.pairs = p->next;

		struct jx *oldvalue = jx_remove(current,p->key);
		if(oldvalue) jx_delete(oldvalue);

		p->next = current->u.pairs;
		current->u.pairs = p;
	}
}

int deltadb_merge_event( struct deltadb_query *db, const char *key, struct jx *update )
{
	struct jx *current = hash_table_lookup(db->table,key);
	if(!current) {
		/* If the key is not found, it was filtered out; skip the update. */
		jx_delete(update);
		return 1;
	}

	if(db->display_mode==DELTADB_DISPLAY_STREAM) {
		display_deferred_time(db);
		char *str = jx_print_string(update);
		fprintf(db->output_stream,"M %s %s\n",key,str);
		free(str);
	}

	jx_merge_into(current,update);

	jx_delete(update);

	return 1;
}

int deltadb_update_event( struct deltadb_query *db, const char *key, const char *name, struct jx *jvalue )
{
	struct jx * jobject = hash_table_lookup(db->table,key);
	if(!jobject) {
		jx_delete(jvalue);
		return 1;
	}

	struct jx *jname = jx_string(name);
	jx_delete(jx_remove(jobject,jname));
	jx_insert(jobject,jname,jvalue);

	if(db->display_mode==DELTADB_DISPLAY_STREAM) {
		display_deferred_time(db);
		char *str = jx_print_string(jvalue);
		fprintf(db->output_stream,"U %s %s %s\n",key,name,str);
		free(str);
	}

	return 1;
}

int deltadb_remove_event( struct deltadb_query *db, const char *key, const char *name )
{
	struct jx *jobject = hash_table_lookup(db->table,key);
	if(!jobject) return 1;

	struct jx *jname = jx_string(name);
	jx_delete(jx_remove(jobject,jname));
	jx_delete(jname);

	if(db->display_mode==DELTADB_DISPLAY_STREAM) {
		display_deferred_time(db);
		fprintf(db->output_stream,"R %s %s\n",key,name);
		return 1;
	}

	return 1;
}

int deltadb_time_event( struct deltadb_query *db, time_t starttime, time_t stoptime, time_t current )
{
	if(current>stoptime) return 0;

	if(current < (db->display_next)) return 1;

	db->display_next += db->display_every;

	if(db->display_mode==DELTADB_DISPLAY_STREAM) {
		db->deferred_time = current;
		return 1;
	} else if(db->display_mode==DELTADB_DISPLAY_OBJECT) {
		display_output_exprs(db,current);
	} else if(db->display_mode==DELTADB_DISPLAY_REDUCE) {
		display_reduce_exprs(db,current);
	}

	return 1;
}

int deltadb_post_event( struct deltadb_query *db, const char *line )
{
	return 1;
}

static int is_leap_year( int y )
{
	return (y%400==0) || ( (y%4==0) && (y%100!=0) );
}

static int days_in_year( int y )
{
	if(is_leap_year(y)) {
		return 366;
	} else {
		return 365;
	}
}

/*
Execute a query on a single data stream.
*/

int deltadb_query_execute_stream( struct deltadb_query *db, FILE *stream, time_t starttime, time_t stoptime )
{
	db->display_next = starttime;
	return deltadb_process_stream(db,stream,starttime,stoptime);
}

/*
Execute a query on a directory structure.
Play the log from starttime to stoptime by opening the appropriate
checkpoint file and working ahead in the various log files.
*/

int deltadb_query_execute_dir( struct deltadb_query *db, const char *logdir, time_t starttime, time_t stoptime )
{
	int file_errors = 0;

	db->display_next = starttime;

	struct tm *starttm = localtime(&starttime);

	int year = starttm->tm_year + 1900;
	int day = starttm->tm_yday;

	struct tm *stoptm = localtime(&stoptime);

	int stopyear = stoptm->tm_year + 1900;
	int stopday = stoptm->tm_yday;

	char *filename = string_format("%s/%d/%d.ckpt",logdir,year,day);
	checkpoint_read(db,filename);
	free(filename);

	while(1) {
		char *filename = string_format("%s/%d/%d.log",logdir,year,day);
		FILE *file = fopen(filename,"r");
		if(!file) {
			file_errors += 1;
			fprintf(stderr,"couldn't open %s: %s\n",filename,strerror(errno));
			free(filename);
			if (file_errors>5)
				break;

		} else {
			free(filename);
			int keepgoing = deltadb_process_stream(db,file,starttime,stoptime);
			starttime = 0;

			fclose(file);

			// If we reached the endtime in the file, stop.
			if(!keepgoing) break;
		}

		day++;
		if(day>=days_in_year(year)) {
			year++;
			day = 0;
		}

		// If we have passed the file, stop.
		if(year>=stopyear && day>stopday) break;
	}

	return 1;
}


