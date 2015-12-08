/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_stream.h"

#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"

#include "hash_table.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

struct argument {
	int dynamic;
	char operator[32];
	char *param;
	char *val;
	struct argument *next;
};

struct deltadb {
	struct hash_table *table;
	struct argument *args;
};

struct deltadb * deltadb_create()
{
	struct deltadb *db = malloc(sizeof(*db));
	db->table = hash_table_create(0,0);
	db->args = NULL;
	return db;
}

static int is_number(char const* p)
{
	char* end;
	strtod(p, &end);
	return !*end;
}

static int jx_is_number( struct jx * j )
{
	return j->type==JX_DOUBLE || j->type==JX_INTEGER;
}

static double jx_to_double( struct jx *j )
{
	if(j->type==JX_DOUBLE) return j->double_value;
	return j->integer_value;
}

static int expr_is_true( struct argument *arg, struct jx *jvalue )
{
	char *operator = arg->operator;
	int cmp;

	/// XXX need to handle other combinations of values here

	if (is_number(arg->val) && jx_is_number(jvalue) ) {
		double in = jx_to_double(jvalue);
		double v = atof(arg->val);
		if (in<v) cmp = -1;
		else if (in==v) cmp = 0;
		else cmp = 1;
	} else {
		cmp = strcmp(jvalue->string_value,arg->val);
	}

	if(strcmp(operator,"=")==0) {
		if(cmp==0)
			return 1;
		else return 0;
	} else if(strcmp(operator,"!=")==0) {
		if(cmp!=0)
			return 1;
		else return 0;
	} else if(strcmp(operator,">")==0) {
		if(cmp>0)
			return 1;
		else return 0;
	} else if(strcmp(operator,">=")==0) {
		if(cmp>=0)
			return 1;
		else return 0;
	} else if(strcmp(operator,"<")==0) {
		if(cmp<0)
			return 1;
		else return 0;
	} else if(strcmp(operator,"<=")==0) {
		if(cmp<=0)
			return 1;
		else return 0;
	}
	return 0;
}

static int object_matches( struct deltadb *db, struct jx *jobject )
{
	struct argument *arg;

	for(arg=db->args;arg;arg=arg->next) {
		struct jx *jvalue = jx_lookup(jobject,arg->param);
		if( jvalue && expr_is_true(arg,jvalue) ) {
			return 1;
			break;
		}
	}

	return 0;
}

int deltadb_create_event( struct deltadb *db, const char *key, struct jx *jobject )
{
	if(object_matches(db,jobject)) {
		hash_table_insert(db->table,key,jobject);
		char *str = jx_print_string(jobject);
		printf("C %s %s\n",key,str);
		free(str);
	}
	return 1;
}

int deltadb_delete_event( struct deltadb *db, const char *key )
{
	struct jx *jobject = hash_table_remove(db->table,key);
	if(jobject) {
		jx_delete(hash_table_remove(db->table,key));
		printf("D %s\n",key);
	}
	return 1;
}

int deltadb_update_event( struct deltadb *db, const char *key, const char *name, struct jx *jvalue )
{
	struct jx *jobject;
	jobject = hash_table_lookup(db->table,key);
	if(jobject) {
		struct jx *jname = jx_string(name);
		jx_delete(jx_remove(jobject,jname));
		jx_insert(jobject,jname,jvalue);

		char *str = jx_print_string(jvalue);
		printf("U %s %s %s\n",key,name,str);
		free(str);
	} else {
		jx_delete(jvalue);
	}
	return 1;
}

int deltadb_remove_event( struct deltadb *db, const char *key, const char *name )
{
	struct jx *jobject;
	jobject = hash_table_lookup(db->table,key);
	if(jobject) {
		struct jx *jname = jx_string(name);
		jx_delete(jx_remove(jobject,jname));
		jx_delete(jname);
		printf("R %s %s\n",key,name);
	}
	return 1;
}

int deltadb_time_event( struct deltadb *db, time_t starttime, time_t stoptime, time_t current )
{
	printf("T %lld\n",(long long)current);
	return 1;
}

int deltadb_post_event( struct deltadb *db, const char *line )
{
	return 1;
}

int main( int argc, char *argv[] )
{
	struct deltadb *db = deltadb_create();

	int i;
	for (i=1; i<argc; i++){
		struct argument arg;

		arg.param = argv[i];
		char *delim = strpbrk(argv[i], "<>=!");
		arg.val = strpbrk(delim, "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");

		int operator_size = (int)(arg.val-delim);
		strncpy(arg.operator,delim,operator_size);
		arg.operator[operator_size] = '\0';
		delim[0] = '\0';
		arg.next = db->args;
		db->args = &arg;
	}

	deltadb_process_stream(db,stdin,0,0);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
