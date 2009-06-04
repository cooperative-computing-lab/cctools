/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "xmalloc.h"
#include "buffer.h"
#include "ftsh_error.h"
#include "list.h"
#include "stringtools.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>

struct vstack {
	int argc;
	char **argv;
	char *rval;
	struct vstack *next;
};

static struct vstack *head=0;
static int vstackdepth=0;

#define ISVALID(x) (isalpha(x) || isdigit(x) || (x=='_') || (x=='$') || (x=='#') || (x=='@')  || (x=='*') )

static int isvalid( const char *n )
{
	while(*n) {
		if(!ISVALID((int)(*n))) return 0;
		n++;
	}
	return 1;
}

int variable_frame_push( int line, int argc, char **argv )
{
	struct vstack *v;
	int i;

	if(vstackdepth>1000) {
		ftsh_error(FTSH_ERROR_FAILURE,line,"aborting: you have recursed %d times",vstackdepth);
		return 0;
	}

	v = malloc(sizeof(*v));
	if(!v) {
		ftsh_error(FTSH_ERROR_FAILURE,line,"out of memory");
		return 0;
	}

	v->argc = argc;
	v->argv = argv;
	v->rval = 0;
	v->next = head;

	for( i=0; i<argc; i++ ) {
		ftsh_error(FTSH_ERROR_STRUCTURE,line,"${%d} = %s",i,argv[i]);
	}

	head = v;
	vstackdepth++;

	return 1;
}

void variable_frame_pop()
{
	struct vstack *v;

	v = head;

	if(!v || !v->next ) {
		ftsh_fatal(0,"stack underflow");
	}

	head = v->next;
	free(v);
	vstackdepth--;
}

void   variable_rval_set( char *rval )
{
	head->rval = rval;
}

char * variable_rval_get()
{
	return head->rval;
}

int variable_shift( int n, int line )
{
	int i;

	if(head->argc>=n) {
		head->argc-=n;
		for(i=0;i<head->argc;i++) {
			head->argv[i] = head->argv[i+n];
		}
		return 1;
	} else {
		ftsh_error(FTSH_ERROR_SYNTAX,line,"cannot shift %d arguments; there are only %d",n,head->argc);
		return 0;
	}
}

static char * variable_print_argv( int withquotes )
{
	char *result=0;
	int i;

	result = xstrdup("");

	for(i=1;i<head->argc;i++) {
		result = string_combine(result,xstrdup(head->argv[i]));
		if(i!=(head->argc-1)) {
			if(withquotes) {
				result = string_combine(result,xstrdup("\" \""));
			} else {
				result = string_combine(result,xstrdup(" "));
			}
		}
	}

	return result;
}

static char * variable_get( const char *name, int line, int withquotes )
{
	char buffer[100];
	int arg;

	if(!strcmp(name,"$")) {
		sprintf(buffer,"%d",(int)getpid());
		return xstrdup(buffer);
	} else if(!strcmp(name,"#")) {
		sprintf(buffer,"%d",head->argc-1);
		return xstrdup(buffer);
	} else if(!strcmp(name,"@")) {
		return variable_print_argv(withquotes);
	} else if(!strcmp(name,"*")) {
		return variable_print_argv(0);
	} else if( sscanf(name,"%d",&arg)==1 ) {
		if(arg>=head->argc) {
			return xstrdup("");
		} else {
			return xstrdup(head->argv[arg]);
		}
	} else if( isvalid(name) ) {
		char *result = getenv(name);
		if(result) return xstrdup(result);
		result = buffer_load(name);
		if(result) string_chomp(result);
		return result;
	} else {
		ftsh_fatal(line,"${%s} is an invalid variable name!",name);
		return 0;
	}
}

char * variable_subst( char *value, int line )
{
	char *subvalue, *newvalue;
	char *dollar, *start, *end;
	char terminator, oldend;
	int length;
	int withquotes = 0;
	int escape = 0;
 
	while(1) {

		/* Find a non-escaped dollar */

		for( dollar=value; *dollar; dollar++ ) {
			if(escape) {
				escape = 0;
			} else {
				if(*dollar=='\\') {
					escape = 1;
				} else if(*dollar=='$') {
					break;
				}
			}
		}

		/* If we didn't find it, stop. */

		if(!*dollar) return value;

		/* Is the variable name bracketed? */

		if( *(dollar+1)=='{' ) {
			start = dollar+2;
			terminator = '}';
		} else if( *(dollar+1)=='(' ) {
			start = dollar+2;
			terminator = ')';
		} else {
			start = dollar+1;
			terminator = 0;
		}

		if(terminator) {
			end = strchr(start,terminator);
		} else {
			for(end=start;ISVALID(*end);end++) {
				/* nothing */
			}
		}

		if(terminator && !end) {
			ftsh_error(FTSH_ERROR_FAILURE,line,"variable reference began with %c but didn't end",*(dollar+1));
			return 0;
		}

		if((end-start)<1) {
			ftsh_error(FTSH_ERROR_FAILURE,line,"empty variable reference");
			return 0;
		}

		withquotes =
			(dollar>value && *(dollar-1)=='\"') &&
			(*end) &&
			(terminator
				? *(end+1)=='\"'
				: *end=='\"'
			);

		oldend = *end;
		*end = 0;
 
		subvalue = variable_get(start,line,withquotes);
		*end = oldend;

		if(!subvalue) {
			subvalue = xstrdup("");
		}
 
		length = strlen(value) - (end-dollar) + strlen(subvalue) + 1;

		newvalue = malloc(length);
		if(!newvalue) {
			free(subvalue);
			free(value);
			return 0;
		}

		*dollar = 0;

		strcpy(newvalue,value);
		strcat(newvalue,subvalue);
		if(terminator && *end) {
			strcat(newvalue,end+1);
		} else {
			strcat(newvalue,end);
		}
		free(subvalue);
		free(value);

		value = newvalue;
	}
}

