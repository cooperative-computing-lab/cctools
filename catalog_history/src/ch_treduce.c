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
#include <ctype.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>


struct reducer {
	int CNT;
	int SUM;
	int FIRST;
	int LAST;
	int MIN;
	int AVG;
	int MAX;
	int PAVG;
	int INC;
};
struct reducer *reducer_create()
{
	struct reducer *r;
	r = malloc(sizeof(*r));
	return r;
};



struct reduction {
	int cnt;
	double sum;
	double first;
	double last;
	double min;
	double avg;
	double max;
	double pavg;
	double inc;
	char *str;

	//Keep track of changes?
	//int CNT:1;
    //int SUM:1;
    //int FIRST:1;
    //int LAST:1;
    //int MIN:1;
    //int AVG:1;
    //int MAX:1;
    //int PAVG:1;
    //int INC:1;
	int dead;
	int gone;
	int is_number;

	struct reducer *reduce;
};
struct reduction *reduction_create()
{
	struct reduction *r;
	r = malloc(sizeof(*r));
	r->str = malloc(sizeof(char)*256);
	return r;
};


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
} reduction2_t;


char *reduction_str(struct reduction *r, reduction2_t type){
	if (!r->is_number || r->cnt<=0)
		return r->str;
	double val;
	switch (type){
		case CNT:
			val = r->cnt;
			break;
		case SUM:
			val = r->sum;
			break;
		case FIRST:
			val = r->first;
			break;
		case LAST:
			val = r->last;
			break;
		case MIN:
			val = r->min;
			break;
		case AVG:
			val = r->avg;
			break;
		case MAX:
			val = r->max;
			break;
		case PAVG:
			val = r->pavg;
			break;
		case INC:
			val = r->inc;
			break;
	}
	//int val2 = val;
	sprintf(r->str,"%.15g",val);
	return r->str;
}

int isnumeric(char *str){
	int decimal_point = 0;
	while(*str) {
		if(!isdigit(*str)){
			if (str[0]=='.' && decimal_point==0){
				decimal_point = 1;
			} else return 0;
	}
	str++;
}

  return 1;
}

void reduction_init(struct reduction *r, char *value)
{
	float val;
	strcpy(r->str,value);

	if (isnumeric(r->str)){
		sscanf(value,"%f",&val);
		r->cnt = 1;
		r->sum = r->first = r->last = r->min = r->avg = r->max = r->pavg = val;
		r->inc = 0;
		r->is_number = 1;
	} else {

		r->is_number = 0;
	}

	r->dead = 0;
	r->gone = 0;
	//r->CNT = r->SUM = r->FIRST = r->LAST = r->MIN = r->AVG = r->MAX = r->PAVG = r->INC = 0;
};
void reduction_update(struct reduction *r, char *value)
{
	float val;
	//if (is_number(value) && r->str==NULL){
	//if (is_number(value) && r->is_number){
	if (isnumeric(r->str) && r->is_number){
		//char* end;
		sscanf(value,"%f",&val);
		r->cnt += 1;
		r->sum += val;
		r->last = val;
		if (val < r->min)
			r->min = val;
		else if (val > r->max)
			r->max = val;
		//printf("%li/%i=%li\n",r->sum,r->cnt,(r->sum/r->cnt));
		r->avg = (r->sum/r->cnt);
		r->pavg = r->avg;
		r->inc = r->last - r->first;
		r->is_number = 1;
	} else {
		strcpy(r->str,value);
		r->is_number = 0;
	}
	r->dead = 0;
	r->gone = 0;
};
void reduction_done(struct reduction *r)
{
	if (r->is_number){
		r->cnt = 1;
		r->sum = r->first = r->last = r->min = r->avg = r->max = r->pavg = r->last;
		r->inc = 0;
	}
};

void reduction_delete(struct reduction *r)
{
	if(!r) return;
	free(r);
}


struct object_status {
	char *key;
	struct hash_table *pairs;
	int dead;
	int gone;
	int new;
};
struct object_status *object_status_create()
{
	struct object_status *s;
	s = malloc(sizeof(*s));
	s->key = malloc(sizeof(char)*64);
	s->key[0] = '\0';
	s->pairs = hash_table_create(7, hash_string);
	s->dead = s->gone = 0;
	s->new = 1;
	return s;
}
void object_status_delete(struct object_status *s)
{
	char *key;
	void *value;

	if(!s) return;

	hash_table_firstkey(s->pairs);
	while(hash_table_nextkey(s->pairs, &key, &value)) {
		hash_table_remove(s->pairs, key);
		free(value);
	}
	hash_table_delete(s->pairs);
	free(s->key);
	free(s);
}
#define NVPAIR_LINE_MAX 1024

int object_status_parse_stream(struct object_status *s, FILE * stream, struct hash_table *reducers, char *key)
{
	int num_pairs = 0;
	char line[NVPAIR_LINE_MAX];
	char name[NVPAIR_LINE_MAX];
	char value[NVPAIR_LINE_MAX];

	//if (key)
		//printf("key %s\n",key);

	while(fgets(line, sizeof(line), stream)) {
		if(line[0] == '.') {
			return -1;
		} else if(line[0] == '\n') {
			if (reducers!=NULL) printf("\n");
			return num_pairs;
		}

		if(sscanf(line, "%s %[^\r\n]", name, value) == 2) {
			if (reducers!=NULL){
				const struct reducer *red = hash_table_lookup(reducers,name);
				if (red){
					if (red->CNT) printf("%s.CNT %s\n",name,value);
					if (red->SUM) printf("%s.SUM %s\n",name,value);
					if (red->MIN) printf("%s.MIN %s\n",name,value);
					if (red->AVG) printf("%s.AVG %s\n",name,value);
					if (red->MAX) printf("%s.MAX %s\n",name,value);
					if (red->FIRST) printf("%s.FIRST %s\n",name,value);
					if (red->LAST) printf("%s.LAST %s\n",name,value);
					if (red->PAVG) printf("%s.PAVG %s\n",name,value);
					if (red->INC) printf("%s.INC %s\n",name,value);
				} else printf("%s",line);
			}
			//fflush(stdout);
			if (strcmp(name,"key")==0){
				strcpy(s->key,value);
			}
			struct reduction *red = reduction_create();
			reduction_init(red,value);
			hash_table_insert(s->pairs, name, red);
			num_pairs += 1;
		} else {
			return 0;
		}

	}

	return 0;
}




struct deltadb {
	struct hash_table *table;
	struct hash_table *reducers;
	long time_span;
	time_t end_span;
};

struct deltadb * deltadb_create(long time_span )
{
	struct deltadb *db = malloc(sizeof(*db));
	db->table = hash_table_create(0,0);
	db->reducers = hash_table_create(0,0);
	db->time_span = time_span;
	db->end_span = 0;
	//db->end_span = start_time + db->time_span;
	return db;
}

void deltadb_delete( struct deltadb *db )
{
	// should delete all nvpairs in the table here
	if(db->table) hash_table_delete(db->table);
	free(db);
}



static int checkpoint_read( struct deltadb *db )
{
	FILE * file = stdin;
	if(!file) return 0;

	char firstline[NVPAIR_LINE_MAX];
	fgets(firstline, sizeof(firstline), file);
	time_t current = atoi(firstline+2);
	db->end_span = current + db->time_span;
	printf("%s",firstline);


	while(1) {
		struct object_status *s = object_status_create();
		int num_pairs = object_status_parse_stream(s,file,db->reducers,NULL);
		if(num_pairs>0) {
			hash_table_insert(db->table,s->key,s);
			s->new = 0;
		} else if (num_pairs == -1) {
			object_status_delete(s);
			return 1;
		} else {
			object_status_delete(s);
		}
	}
	return 1;
}

/*
Replay a given log file into the hash table, up to the given snapshot time.
Return true if the stoptime was reached.
*/

static int log_play( struct deltadb *db  )
{
	FILE *stream = stdin;
	time_t current = 0;
	struct object_status *s;
	int line_number = 0;
	struct hash_table *table = db->table;

	char line[NVPAIR_LINE_MAX];
	char key[NVPAIR_LINE_MAX];
	char name[NVPAIR_LINE_MAX];
	char value[NVPAIR_LINE_MAX];
	char oper;
	struct reduction *red;
	char *keyp;
	char *namep;
	void *sp;
	void *valp;
	int finishUp = 0;
	
	while(fgets(line,sizeof(line),stream)) {
		//debug(D_NOTICE,"Processed line: %s",line);
		//fflush(stderr);

		line_number += 1;
		
		if (line[0]!='.'){
			int n = sscanf(line,"%c %s %s %[^\n]",&oper,key,name,value);
			if(n<1) continue;

		} else oper = '.';
		
		
		//int include;
		switch(oper) {
			case 'C':
				object_status_delete( hash_table_remove(db->table,key) );
				s = object_status_create();
				int num_pairs = object_status_parse_stream(s,stream,NULL,key);
				if(num_pairs>0) {
					hash_table_insert(db->table,key,s);
				} else if (num_pairs == -1) {
					object_status_delete(s);
					return 1;
				} else {
					object_status_delete(s);
				}
				break;
			case 'D':
				s = hash_table_lookup(table,key);
				if(s) {
					s->dead = 1;
				}
				break;
			case 'U':
				s = hash_table_lookup(table,key);
				if(s) {
					struct reduction *r = hash_table_lookup(s->pairs,name);
					if (!r) {
						r = reduction_create();
						reduction_init(r,value);
						hash_table_insert(s->pairs, name, r);
					} else reduction_update(r,value);
				}
				break;
			case 'R':
				s = hash_table_lookup(table,key);
				if(s) {
					struct reduction *r = hash_table_lookup(s->pairs,name);
					if (r){
						r->dead = 1;
					}
				}
				break;
			case '.':
				sprintf(key,"%lld",(long long)db->end_span+1);
				finishUp = 1;
			case 'T':
				current = atol(key);
				while (current > db->end_span){
					//Flush Span
					printf("T %lld\n",(long long)db->end_span-1);

					hash_table_firstkey(table);
					while(hash_table_nextkey(table, &keyp, &sp)) {

						s = sp;
						if (s->dead && s->gone){
							printf("D %s\n",keyp);
							hash_table_remove(table,keyp);
							object_status_delete(s);
						} else if (s->new==1){
							printf("C %s\n",keyp);
							hash_table_firstkey(s->pairs);
							while(hash_table_nextkey(s->pairs, &namep, &valp)) {
								red = valp;
								if (!red->is_number){
									printf("%s %s\n",namep,red->str);
									//reduction_init(red,red->str); //Not necessary because there is no reduction on strings
								} else {
									const struct reducer *r = hash_table_lookup(db->reducers,namep);
									if (r){
										if (r->CNT) printf("%s.CNT %s\n",namep,reduction_str(red,CNT));
										if (r->SUM) printf("%s.SUM %s\n",namep,reduction_str(red,SUM));
										if (r->MIN) printf("%s.MIN %s\n",namep,reduction_str(red,MIN));
										if (r->AVG) printf("%s.AVG %s\n",namep,reduction_str(red,AVG));
										if (r->MAX) printf("%s.MAX %s\n",namep,reduction_str(red,MAX));
										if (r->FIRST) printf("%s.FIRST %s\n",namep,reduction_str(red,FIRST));
										if (r->LAST) printf("%s.LAST %s\n",namep,reduction_str(red,LAST));
										if (r->PAVG) printf("%s.PAVG %s\n",namep,reduction_str(red,PAVG));
										if (r->INC) printf("%s.INC %s\n",namep,reduction_str(red,INC));
										reduction_done(red);
									} else printf("%s %s\n",namep,reduction_str(red,LAST));
									//reduction_end(red);
								}
								if(red->dead){
									red->gone = 1;
								}
							}
							if (s->new==1){
								printf("\n");
								s->new = 0;
							}
						} else {
							hash_table_firstkey(s->pairs);
							while(hash_table_nextkey(s->pairs, &namep, &valp)) {
								red = valp;
								if (red->dead && red->gone){
									printf("R %s %s\n",keyp,namep);
									reduction_delete(hash_table_remove(s->pairs,namep));
								} else {
									if (!red->is_number){
										printf("U %s %s\n",namep,red->str);
										//reduction_init(red,red->str); //Not necessary because there is no reduction on strings
									} else {
										const struct reducer *r = hash_table_lookup(db->reducers,namep);
										if (r){

											if (r->CNT) printf("U %s %s.CNT %s\n",keyp,namep,reduction_str(red,CNT));
											if (r->SUM) printf("U %s %s.SUM %s\n",keyp,namep,reduction_str(red,SUM));
											if (r->MIN) printf("U %s %s.MIN %s\n",keyp,namep,reduction_str(red,MIN));
											if (r->AVG) printf("U %s %s.AVG %s\n",keyp,namep,reduction_str(red,AVG));
											if (r->MAX) printf("U %s %s.MAX %s\n",keyp,namep,reduction_str(red,MAX));
											if (r->FIRST) printf("U %s %s.FIRST %s\n",keyp,namep,reduction_str(red,FIRST));
											if (r->LAST) printf("U %s %s.LAST %s\n",keyp,namep,reduction_str(red,LAST));
											if (r->PAVG) printf("U %s %s.PAVG %s\n",keyp,namep,reduction_str(red,PAVG));
											if (r->INC) printf("U %s %s.INC %s\n",keyp,namep,reduction_str(red,INC));
											reduction_done(red);
										} else printf("U %s %s %s\n",keyp,namep,reduction_str(red,LAST));

									}
								}
							}
						}


						if(s!=NULL && s->dead){
							s->gone = 1;
						}

					}





					db->end_span += db->time_span;
				}
				if (finishUp){
					printf("T %lld\n",(long long)db->end_span-1);

					hash_table_firstkey(table);
					while(hash_table_nextkey(table, &keyp, &sp)) {
						if (s->dead && s->gone){
							printf("D %s\n",keyp);
							hash_table_remove(table,keyp);
							object_status_delete(s);
						} else {
							hash_table_firstkey(s->pairs);
							while(hash_table_nextkey(s->pairs, &namep, &valp)) {
								red = valp;
								if (red->dead && red->gone){
									printf("R %s %s\n",keyp,namep);
									reduction_delete(hash_table_remove(s->pairs,namep));
								}
							}
						}
					}
					return 0;
				}
				break;
			default:
				debug(D_NOTICE,"corrupt log data[%i]: %s",line_number,line);
				fflush(stderr);
				break;
		}

	}
	return 1;
}



/*
Play the log from start_time to end_time by opening the appropriate
checkpoint file and working ahead in the various log files.
*/

static int parse_input( struct deltadb *db )
{      
	checkpoint_read(db);
	
	printf(".Checkpoint End.\n");
	
	while(1) {

		int keepgoing = log_play(db);
		
		if(!keepgoing) break;

	}

	printf(".Log End.\n");

	return 1;
}

int main( int argc, char *argv[] )
{
	int i;

	int duration_value;
	char duration_metric;
	long time_span;
	sscanf(argv[1], "%c%i", &duration_metric, &duration_value);
	if (duration_metric=='y')
		time_span = duration_value*365*24*3600;
	else if (duration_metric=='w')
		time_span = duration_value*7*24*3600;
	else if (duration_metric=='d')
		time_span = duration_value*24*3600;
	else if (duration_metric=='h')
		time_span = duration_value*3600;
	else if (duration_metric=='m')
		time_span = duration_value*60;
	else
		time_span = duration_value;


	struct deltadb *db = deltadb_create(time_span);


	for (i=2; i<argc; i++){
		struct reducer *red = reducer_create();

		char *attribute = strtok(argv[i], ",");
		//attr->cnt = attr->sum = attr->first = attr->last = attr->min = attr->avg = attr->max = attr->pavg = attr->inc = NULL;
		red->CNT = red->SUM = red->FIRST = red->LAST = red->MIN = red->AVG = red->MAX = red->PAVG = red->INC = 0;

		char *reducer;
		while( (reducer = strtok(0, ",")) ){
			if (strcmp(reducer,"CNT")==0)
				red->CNT = 1;
			else if (strcmp(reducer,"SUM")==0)
				red->SUM = 1;
			else if (strcmp(reducer,"FIRST")==0)
				red->FIRST = 1;
			else if (strcmp(reducer,"LAST")==0)
				red->LAST = 1;
			else if (strcmp(reducer,"MIN")==0)
				red->MIN = 1;
			else if (strcmp(reducer,"AVG")==0)
				red->AVG = 1;
			else if (strcmp(reducer,"MAX")==0)
				red->MAX = 1;
			else if (strcmp(reducer,"PAVG")==0)
				red->PAVG = 1;
			else if (strcmp(reducer,"INC")==0)
				red->INC = 1;
		}

		hash_table_insert(db->reducers,attribute,red);
	}


	parse_input(db);

	deltadb_delete(db);

	return 0;
}
