#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ragged_array.h"

struct ragged_array ragged_array_initialize(const int size) {
    struct ragged_array retset;
    if(size <= 0) {
	retset.arr = NULL;
	retset.row_count = 0;
	retset.array_size = 0;
	return retset;
    }

    retset.arr = (char **) malloc(size * sizeof(char *));
    if(retset.arr == NULL) {
	fprintf(stderr,"Allocating set failed!\n");
	retset.row_count = 0;
	retset.array_size = 0;
    }
    else {
	retset.row_count = 0;
	retset.array_size = size;
    }
    return retset;
}


int ragged_array_expand(struct ragged_array* set, const int new_size) {
    char** tmpptr;
    tmpptr = realloc(set->arr, new_size * sizeof(char *));
    if(tmpptr == NULL) {
	fprintf(stderr,"Realloc failed!\n");
	return -1;
    }
    else {
	set->arr=tmpptr;
	set->array_size = new_size;
	return 0;
    }
}

struct ragged_array ragged_array_populate(const char *path,const char *prefix, const int line_max) {
    int len = line_max+2;
    char* tmpstr;
    char* addstr;

    struct ragged_array nullset = ragged_array_initialize(0);
    struct ragged_array retset = ragged_array_initialize(100);


    tmpstr = (char *) malloc((len) * sizeof(char));
    if(tmpstr == NULL) {fprintf(stderr,"Allocating input string failed!\n"); return nullset;}

    if(prefix != NULL) {
	addstr = (char*) malloc((strlen(prefix)+1+len)*sizeof(char));
	if(addstr == NULL) {fprintf(stderr,"Allocating input string failed!\n"); return nullset;}
    }
    else {
	addstr = (char*) malloc((len)*sizeof(char));
	if(addstr == NULL) {fprintf(stderr,"Allocating input string failed!\n"); return nullset;}
    }
    
    FILE*  pathFID = fopen(path, "r");
    if(!pathFID) {fprintf(stderr,"Couldn't open %s!\n",path); return nullset;}    
    fgets(tmpstr, len, pathFID);
    if (tmpstr != NULL) {
	size_t last = strlen (tmpstr) - 1;
	if (tmpstr[last] == '\n') tmpstr[last] = '\0';
    }
    
    if(prefix != NULL)
	sprintf(addstr,"%s/%s",prefix,tmpstr);
    else
	sprintf(addstr,"%s",tmpstr);
    
    while(!feof(pathFID)) {
	if(ragged_array_add_line(&retset,addstr) < 0) {
	    fprintf(stderr,"Could not add line %i: %s\n",retset.row_count,addstr);
	    return nullset;
	}

	fgets(tmpstr, len, pathFID);
	if (tmpstr != NULL) {
            size_t last = strlen (tmpstr) - 1;
	    if (tmpstr[last] == '\n') tmpstr[last] = '\0';
	    //printf("set[%i] = %s\n", numset, set[numset]);
	}
	if(prefix != NULL)
	    sprintf(addstr,"%s/%s",prefix,tmpstr);
	else
	    sprintf(addstr,"%s",tmpstr);
    }

    fclose(pathFID);

    return retset;
}

int ragged_array_add_line(struct ragged_array* set, const char* line) {
    if(set->row_count == set->array_size) {
	if(ragged_array_expand(set,2*set->array_size) < 0)
		    return -1;
    }

    set->arr[set->row_count] = (char *) malloc((strlen(line)+1)* sizeof(char));
    if(set->arr[set->row_count] == NULL) {
	fprintf(stderr,"Allocating set[%i] failed!\n",set->row_count);
	return -2;
    }

    sprintf(set->arr[set->row_count],"%s",line);
    set->row_count++;
    return 0;
}


// Delete a line while retaining continuity of the array -- warning: potentially slow!
int ragged_array_delete_line(struct ragged_array* set, const int line_index) {

    int i;
    char* tmp;
    if(!set)
	return 0;

    if(set->row_count <= line_index || set->array_size <= line_index)
	return 0;

    if(line_index+1 == set->row_count) // deleting last line
	set->arr[line_index][0] = 0;
    else {
	tmp=set->arr[line_index];
	for(i=line_index; i<set->row_count-1; i++)
	{
	    set->arr[i] = set->arr[i+1];
	}
	set->arr[set->row_count-1]=tmp;
	set->arr[set->row_count-1][0] = 0;
    }
    set->row_count--;
    return 1;
}

void ragged_array_print( struct ragged_array *t )
{
	int i;

	if(!t)
		return;

	printf("Array size: %d; Elements are as follow:\n", t->array_size);
	for(i = 0; i < t->array_size; i++) {
		printf("\t%s\n", t->arr[i]);
	}
	printf("\n");
}
