/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_stream.h"
#include "deltadb_reduction.h"

#include "jx_eval.h"
#include "jx_database.h"
#include "jx_print.h"
#include "jx_parse.h"

#include "hash_table.h"
#include "debug.h"
#include "getopt.h"
#include "cctools.h"
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

struct deltadb {
	struct hash_table *table;
	const char *logdir;
	FILE *logfile;
	int epoch_mode;
	struct jx *filter_expr;
	struct jx *where_expr;
	struct list * output_exprs;
	struct list * reduce_exprs;
	time_t display_every;
	time_t last_display;
	time_t deferred_time;
};

enum { MODE_STREAM, MODE_OBJECT, MODE_REDUCE } display_mode = MODE_REDUCE;

struct deltadb * deltadb_create( const char *logdir )
{
	struct deltadb *db = malloc(sizeof(*db));
	memset(db,0,sizeof(*db));
	db->table = hash_table_create(0,0);
	db->logdir = logdir;
	db->logfile = 0;
	return db;
}

int deltadb_boolean_expr( struct jx *expr, struct jx *data )
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

static int checkpoint_read( struct deltadb *db, const char *filename )
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

static void display_reduce_exprs( struct deltadb *db, time_t current )
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
		printf("%lld\t",(long long) current);
	} else {
		char str[32];
		strftime(str,sizeof(str),"%F %T",localtime(&current));
		printf("%s\t",str);
	}

	/* For each reduction, display the final value. */
	list_first_item(db->reduce_exprs);
	for(struct deltadb_reduction *r; (r = list_next_item(db->reduce_exprs));) {
		printf("%lf\t",deltadb_reduction_value(r));
	}

	printf("\n");

}

static void display_output_exprs( struct deltadb *db, time_t current )
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
			printf("%lld\t",(long long) current);
		} else {
			char str[32];
			strftime(str,sizeof(str),"%F %T",localtime(&current));
			printf("%s\t",str);
		}

		/* For each output expression, compute the value and print. */

		list_first_item(db->output_exprs);
		for(struct jx *j; (j = list_next_item(db->output_exprs));) {
			struct jx *jvalue = jx_eval(j,jobject);
			jx_print_stream(jvalue,stdout);
			printf("\t");
			jx_delete(jvalue);
		}

		printf("\n");
	}
}

/*
To eliminate unnecessary T record on the output in streaming mode,
we store incoming T records as "deferred time" and then only
output if another record type intervenes.
*/

static void display_deferred_time( struct deltadb *db )
{
	if(db->deferred_time) {
		printf("T %ld\n",db->deferred_time);
		db->deferred_time = 0;
	}
}

int deltadb_create_event( struct deltadb *db, const char *key, struct jx *jobject )
{
	if(!deltadb_boolean_expr(db->filter_expr,jobject)) {
		jx_delete(jobject);
		return 1;
	}

	hash_table_insert(db->table,key,jobject);

	if(display_mode==MODE_STREAM) {
		display_deferred_time(db);
		printf("C %s ",key);
		jx_print_stream(jobject,stdout);
		printf("\n");
	}
	return 1;
}

int deltadb_delete_event( struct deltadb *db, const char *key )
{
	struct jx *jobject = hash_table_remove(db->table,key);

	if(jobject) {
		jx_delete(jobject);

		if(display_mode==MODE_STREAM) {
			display_deferred_time(db);
			printf("D %s\n",key);
		}
	}
	return 1;
}

int deltadb_merge_event( struct deltadb *db, const char *key, struct jx *update )
{
	struct jx *current = hash_table_remove(db->table,key);
	if(!current) {
		/* If the key is not found, it was filtered out; skip the update. */
		jx_delete(update);
		return 1;
	}

	struct jx * merged = jx_merge(update,current,0);

	hash_table_insert(db->table,key,merged);

	if(display_mode==MODE_STREAM) {
		display_deferred_time(db);
		char *str = jx_print_string(update);
		printf("M %s %s\n",key,str);
		free(str);
	}

	jx_delete(update);
	jx_delete(current);

	return 1;
}

int deltadb_update_event( struct deltadb *db, const char *key, const char *name, struct jx *jvalue )
{
	struct jx * jobject = hash_table_lookup(db->table,key);
	if(!jobject) {
		jx_delete(jvalue);
		return 1;
	}

	struct jx *jname = jx_string(name);
	jx_delete(jx_remove(jobject,jname));
	jx_insert(jobject,jname,jvalue);

	if(display_mode==MODE_STREAM) {
		display_deferred_time(db);
		char *str = jx_print_string(jvalue);
		printf("U %s %s %s\n",key,name,str);
		free(str);
	}

	return 1;
}

int deltadb_remove_event( struct deltadb *db, const char *key, const char *name )
{
	struct jx *jobject = hash_table_lookup(db->table,key);
	if(!jobject) return 1;

	struct jx *jname = jx_string(name);
	jx_delete(jx_remove(jobject,jname));
	jx_delete(jname);

	if(display_mode==MODE_STREAM) {
		display_deferred_time(db);
		printf("R %s %s\n",key,name);
		return 1;
	}

	return 1;
}

int deltadb_time_event( struct deltadb *db, time_t starttime, time_t stoptime, time_t current )
{
	if(current>stoptime) return 0;

	if(current < (db->last_display + db->display_every)) return 1;

	db->last_display = current;

	if(display_mode==MODE_STREAM) {
		db->deferred_time = current;
		return 1;
	} else if(display_mode==MODE_OBJECT) {
		display_output_exprs(db,current);
	} else if(display_mode==MODE_REDUCE) {
		display_reduce_exprs(db,current);
	}

	return 1;
}

int deltadb_post_event( struct deltadb *db, const char *line )
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
Play the log from starttime to stoptime by opening the appropriate
checkpoint file and working ahead in the various log files.
*/

static int log_play_time( struct deltadb *db, time_t starttime, time_t stoptime )
{
	int file_errors = 0;

	struct tm *starttm = localtime(&starttime);

	int year = starttm->tm_year + 1900;
	int day = starttm->tm_yday;

	struct tm *stoptm = localtime(&stoptime);

	int stopyear = stoptm->tm_year + 1900;
	int stopday = stoptm->tm_yday;

	char *filename = string_format("%s/%d/%d.ckpt",db->logdir,year,day);
	checkpoint_read(db,filename);
	free(filename);

	while(1) {
		char *filename = string_format("%s/%d/%d.log",db->logdir,year,day);
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

	n = sscanf(str, "%d-%d-%d %d:%d:%d", &t.tm_year,&t.tm_mon,&t.tm_mday,&t.tm_hour,&t.tm_min,&t.tm_sec);
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

static struct option long_options[] =
{
	{"db", required_argument, 0, 'D'},
	{"file", required_argument, 0, 'L'},
	{"output", required_argument, 0, 'o'},
	{"where", required_argument, 0,'w'},
	{"filter", required_argument, 0,'f'},
	{"from", required_argument, 0, 'F'},
	{"to", required_argument, 0, 'T'},
	{"at", required_argument, 0, 'A'},
	{"every", required_argument, 0, 'e'},
	{"epoch", no_argument, 0, 't'},
	{"version", no_argument, 0, 'v'},
	{"help", no_argument, 0, 'h'},
	{0,0,0,0}
};

void show_help()
{
	printf("use: deltadb_query [options]\n");
	printf("Where options are:\n");
	printf("  --db <path>         Query this database directory.\n");
	printf("  --file <path>       Query this raw data file.\n");
	printf("  --output <expr>     Output this expression. (multiple)\n");
	printf("  --where <expr>      Only output records matching this expression.\n");
	printf("  --filter <expr>     Only process records matching this expression.\n");
	printf("  --from <time>       Begin query at this absolute time. (required)\n");
	printf("  --to <time>         End query at this absolute time.\n");
	printf("  --every <interval>  Compute output at this time interval.\n");
	printf("  --epoch             Display time column in Unix epoch format.\n");
	printf("  --version           Show software version.\n");
	printf("  --help              Show this help text.\n");
}

int main( int argc, char *argv[] )
{
	const char *dbdir=0;
	const char *dbfile=0;
	struct jx *where_expr = 0;
	struct jx *filter_expr = 0;
	struct list *output_exprs = list_create();
	struct list *reduce_exprs = list_create();
	time_t start_time = 0;
	time_t stop_time = 0;
	int display_every = 0;
	int epoch_mode = 0;

	char reduce_name[1024];
	char reduce_attr[1024];

	time_t current = time(0);

	int c;

	while((c=getopt_long(argc,argv,"D:L:o:w:f:F:T:e:tvh",long_options,0))!=-1) {
		switch(c) {
		case 'D':
			dbdir = optarg;
			break;
		case 'L':
			dbfile = optarg;
			break;
		case 'o':
			if(2==sscanf(optarg,"%[^(](%[^)])",reduce_name,reduce_attr)) {

				struct jx *reduce_expr = jx_parse_string(reduce_attr);
				if(!reduce_expr) {
					fprintf(stderr,"deltadb_query: invalid expression: %s\n",reduce_attr);
					return 1;
				}

				struct deltadb_reduction *r = deltadb_reduction_create(reduce_name,reduce_expr);
				if(!r) {
					fprintf(stderr,"deltadb_query: invalid reduction: %s\n",reduce_name);
					return 1;
				}
				list_push_tail(reduce_exprs,r);
			} else {
				struct jx *j = jx_parse_string(optarg);
				if(!j) {
					fprintf(stderr,"invalid expression: %s\n",optarg);
					return 1;
				}
				list_push_tail(output_exprs,j);
			}
			break;
		case 'w':
			if(where_expr) {
				fprintf(stderr,"Only one --where expression is allowed.  Try joining the expressions with the && (and) operator.");
				return 1;
			}
			where_expr = jx_parse_string(optarg);
			if(!where_expr) {
				fprintf(stderr,"invalid expression: %s\n",optarg);
				return 1;
			}
			break;
		case 'f':
			if(filter_expr) {
				fprintf(stderr,"Only one --filter expression is allowed.  Try joining the expressions with the && (and) operator.");
				return 1;
			}
			filter_expr = jx_parse_string(optarg);
			if(!filter_expr) {
				fprintf(stderr,"invalid expression: %s\n",optarg);
				return 1;
			}
			break;
		case 'F':
			start_time = parse_time(optarg,current);
			break;
		case 'T':
			stop_time = parse_time(optarg,current);
			break;
		case 'e':
			display_every = string_time_parse(optarg);
			break;
		case 't':
			epoch_mode = 1;
			break;
		case 'v':
			cctools_version_print(stdout,"deltadb_query");
			break;
		case 'h':
			show_help();
			break;
		}
	}

	if(!dbdir && !dbfile) {
		fprintf(stderr,"deltadb_query: either --db or --file argument is required\n");
		return 1;
	}

	if(start_time==0) {
		fprintf(stderr,"deltadb_query: invalid --from time (must be \"YY-MM-DD\" or \"YY-MM-DD HH:MM:SS\")\n");
		return 1;
	}

	if(stop_time==0) {
		stop_time = time(0);
	}

	struct deltadb *db = deltadb_create(dbdir);

	db->where_expr = where_expr;
	db->filter_expr = filter_expr;
	db->epoch_mode = epoch_mode;
	db->output_exprs = output_exprs;
	db->reduce_exprs = reduce_exprs;
	db->display_every = display_every;

	if(list_size(db->reduce_exprs) && list_size(db->output_exprs) ) {
		struct deltadb_reduction *r = list_peek_head(db->reduce_exprs);
		const char *name = jx_print_string(list_peek_head(db->output_exprs));
		fprintf(stderr,"deltadb_query: cannot mix reductions like 'MAX(%s)' with plain outputs like '%s'\n",jx_print_string(r->expr),name);
		return 1;
	}

	if(list_size(db->reduce_exprs)) {
		display_mode = MODE_REDUCE;
	} else if(list_size(db->output_exprs)) {
		display_mode = MODE_OBJECT;
	} else {
		display_mode = MODE_STREAM;
	}

	if(dbfile) {
		FILE *file = fopen(dbfile,"r");
		if(!file) {
			fprintf(stderr,"deltadb_query: couldn't open %s: %s\n",dbfile,strerror(errno));
			return 1;
		}
		deltadb_process_stream(db,file,start_time,stop_time);
		fclose(file);
	} else {
		log_play_time(db,start_time,stop_time);
	}

	return 0;
}
