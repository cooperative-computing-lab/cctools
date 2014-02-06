/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "nvpair.h"
#include "hash_table.h"
#include "debug.h"
#include "limits.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

typedef enum {
	CNT,
	SUM,
	FIRST,
	LAST,
	MIN,
	AVG,
	MAX,
	PAVG,
	INC
} reduction_t;

struct reduction {
	char *attr;
	reduction_t type;
	long cnt;
	long sum;
	long first;
	long last;
	long min;
	long max;
};

struct reduction *reduction_create( const char *name, const char *attr )
{
	struct reduction *r;
	reduction_t type;

	if (strcmp(name,"CNT")==0)		type = CNT;
	else if (strcmp(name,"SUM")==0)		type = SUM;
	else if (strcmp(name,"FIRST")==0)	type = FIRST;
	else if (strcmp(name,"LAST")==0)	type = LAST;
	else if (strcmp(name,"MIN")==0)		type = MIN;
	else if (strcmp(name,"AVG")==0)		type = AVG;
	else if (strcmp(name,"MAX")==0)		type = MAX;
	else if (strcmp(name,"PAVG")==0)	type = PAVG;
	else if (strcmp(name,"INC")==0)		type = INC;
	else	return 0;

	r = malloc(sizeof(*r));
	memset(r,0,sizeof(*r));
	r->type = type;
	r->attr = strdup(attr);
	return r;
};

void reduction_delete( struct reduction *r )
{
	if(!r) return;
	free(r->attr);
	free(r);
}

void reduction_reset( struct reduction *r )
{
	r->cnt = r->sum = r->first = r->last = r->min = r->max = 0;
};

void reduction_update( struct reduction *r, const char *value )
{
	long val = atol(value);

	if(r->cnt==0) {
		r->min = r->max = r->first = val;
	} else {
		if (val < r->min) r->min = val;
		if (val > r->max) r->max = val;
	}

	r->sum += val;
	r->last = val;
	r->cnt++;
};

void reduction_print( struct reduction *r )
{
	printf("%s",r->attr);
	switch(r->type) {
		case CNT:
			printf(".CNT %ld\n",r->cnt);
			break;
		case SUM:
			printf(".SUM %ld\n",r->sum);
			break;
		case FIRST:
			printf(".FIRST %ld\n",r->first);
			break;
		case LAST:
			printf(".LAST %ld\n",r->last);
			break;
		case MIN:
			printf(".MIN %ld\n",r->min);
			break;
		case AVG:
			printf(".AVG %ld\n",r->cnt>0 ? r->sum/r->cnt : 0);
			break;
		case MAX:
			printf(".MAX %ld\n",r->max);
			break;
		case PAVG:
			printf(".PAVG %ld\n",r->cnt>0 ? r->sum/r->cnt : 0 );
			break;
		case INC:
			printf(".INC %ld\n",r->last-r->first);
			break;
	}
}

struct deltadb {
	struct hash_table *table;
	struct reduction *reductions[100];
	int nreductions;
};

struct deltadb * deltadb_create()
{
	struct deltadb *db = malloc(sizeof(*db));
	db->table = hash_table_create(0,0);
	db->nreductions = 0;
	return db;
}

void deltadb_delete( struct deltadb *db )
{
	// should delete all nvpairs in the table here
	if(db->table) hash_table_delete(db->table);
	free(db);
}

static int checkpoint_read( struct deltadb *db, FILE *file )
{
	while(1) {
		int c = fgetc(file);
		if(c=='.') {
			char line[1024];
			fgets(line,sizeof(line),file);
			return 1;
		} else {
			ungetc(c,file);

		}
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

	return 1;
}

#define NVPAIR_LINE_MAX 1024

/*
Replay a given log file into the hash table, up to the given snapshot time.
Return true if the stoptime was reached.
*/
static int log_play( struct deltadb *db, FILE *stream  )
{
	time_t current = 0;
	struct nvpair *nv;

	char line[NVPAIR_LINE_MAX];
	char key[NVPAIR_LINE_MAX];
	char name[NVPAIR_LINE_MAX];
	char value[NVPAIR_LINE_MAX];
	char oper;

	int first_output = 1;
	
	while(fgets(line,sizeof(line),stream)) {

		int n = sscanf(line,"%c %s %s %[^\n]",&oper,key,name,value);
		if(n<1) continue;

		switch(oper) {
			case 'C':
				nv = nvpair_create();
				nvpair_parse_stream(nv,stream);
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
				break;
			default:
				debug(D_NOTICE,"corrupt log data: %s",line);
				break;
		}

		int i;

		/* Reset all reduction state. */
		for(i=0;i<db->nreductions;i++) {
			reduction_reset(db->reductions[i]);
		}

		/* After each event, iterate over all objects... */

		char *hkey;
		hash_table_firstkey(db->table);
		while(hash_table_nextkey(db->table,&hkey,(void**)&nv)) {

			/* Update all reductions for that object. */
			for(i=0;i<db->nreductions;i++) {
				struct reduction *r = db->reductions[i];
				const char *hvalue = nvpair_lookup_string(nv,r->attr);
				if(hvalue) reduction_update(r,hvalue);
			}
		}

		/* Q: What does it mean where there are no items in the current snapshot? */


		/* Optimization: Only display when the value actually changes. */

		if(first_output) {
			printf("key 0 \n");
			for(i=0;i<db->nreductions;i++) {
				struct reduction *r = db->reductions[i];
				reduction_print(r);
			}
			printf("\n");
			printf(".Checkpoint End.\n");
			printf("T %ld\n",(long)current);
			first_output = 0;
		} else {
			printf("T %ld\n",(long)current);
			for(i=0;i<db->nreductions;i++) {
				struct reduction *r = db->reductions[i];
				printf("U 0 ");
				reduction_print(r);
			}
		}
	}
	return 1;
}

int main( int argc, char *argv[] )
{
	int i;

	struct deltadb *db = deltadb_create();

	for (i=1; i<argc; i++){
		char *attr = strtok(argv[i], ",");
		char *type = strtok(0,",");

		struct reduction *r = reduction_create(type,attr);

		if(!r) {
			fprintf(stderr,"%s: invalid reduction: %s\n",argv[0],type);
			return 1;
		}

		db->reductions[db->nreductions++] = r;
	}

	/*
	Need to add a beginning record here.
	 */


	checkpoint_read(db,stdin);
	log_play(db,stdin);
	deltadb_delete(db);

	return 0;
}
