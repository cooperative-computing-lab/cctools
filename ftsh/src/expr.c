/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "expr.h"
#include "ftsh_error.h"
#include "ast_execute.h"
#include "ast_print.h"

#include "xxmalloc.h"
#include "macros.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <limits.h>

static void expr_print_list( FILE *file, struct expr *e, int with_commas );

struct expr_table {
	const char *string;
	enum expr_type_t type;
};

static struct expr_table table[] = {
	{".add.",EXPR_ADD},
	{".sub.",EXPR_SUB},
	{".mul.",EXPR_MUL},
	{".div.",EXPR_DIV},
	{".mod.",EXPR_MOD},
	{".pow.",EXPR_POW},
	{".eq.",EXPR_EQ},
	{".ne.",EXPR_NE},
	{".eql.",EXPR_EQL},
	{".neql.",EXPR_NEQL},
	{".lt.",EXPR_LT},
	{".le.",EXPR_LE},
	{".gt.",EXPR_GT},
	{".ge.",EXPR_GE},
	{".and.",EXPR_AND},
	{".or.",EXPR_OR},
	{".not.",EXPR_NOT},
	{".exists.",EXPR_EXISTS},
	{".isr.",EXPR_ISR},
	{".isw.",EXPR_ISW},
	{".isx.",EXPR_ISX},
	{".isblock.",EXPR_ISBLOCK},
	{".ischar.",EXPR_ISCHAR},
	{".isdir.",EXPR_ISDIR},
	{".isfile.",EXPR_ISFILE},
	{".islink.",EXPR_ISLINK},
	{".ispipe.",EXPR_ISPIPE},
	{".issock.",EXPR_ISSOCK},
	{0,0}
};

const char *expr_type_to_string( enum expr_type_t type )
{
	struct expr_table *t;

	for(t=table;t->string;t++) {
		if(t->type==type) return t->string;
	}

	return 0;
}

static int digits_in_int( int i )
{
	int digits=0;
	do {
		digits++;
		i/=10;
	} while(i>0);
	return digits;
}

struct expr * expr_create( int line, enum expr_type_t type, struct ast_word *literal, struct expr *a, struct expr *b, struct expr *c )
{
	struct expr *e = xxmalloc(sizeof(*e));
	e->line = line;
	e->literal = literal;
	e->type = type;
	e->a = a;
	e->b = b;
	e->c = c;
	e->next = 0;
	return e;
}

int expr_to_integer( struct expr *e, ftsh_integer_t *ival, time_t stoptime )
{
	char *value;
	char *end;

	value = expr_eval(e,stoptime);
	if(!value) return 0;

	*ival = strtol(value,&end,10);
	if(!*end) {
		/* good conversion */
		free(value);
		return 1;
	} else {
		ftsh_error(FTSH_ERROR_FAILURE,e->line,"expected integer but got '%s' instead",value);
		free(value);
		return 0;
	}
}

int expr_to_boolean( struct expr *e, ftsh_boolean_t *bval, time_t stoptime )
{
	char *value;

	value = expr_eval(e,stoptime);
	if(!value) return 0;

	if(!strcmp(value,"true")) {
		free(value);
		*bval = 1;
		return 1;
	} else if(!strcmp(value,"false")) {
		free(value);
		*bval = 0;
		return 1;
	} else {
		ftsh_error(FTSH_ERROR_FAILURE,e->line,"expected 'true' or 'false' but got %s instead",value);
		free(value);
		return 0;
	}
}

static char * integer_to_string( int line, ftsh_integer_t i )
{
	char istr[256];
	char *s;

	sprintf(istr,"%ld",i);
	s = strdup(istr);

	if(!s) {
		ftsh_error(FTSH_ERROR_FAILURE,line,"out of memory");
	}

	return s;
}

static char * boolean_to_string( int line, ftsh_boolean_t b )
{
	char *s;

	if(b) {
		s = strdup("true");
	} else {
		s = strdup("false");
	}

	if(!s) {
		ftsh_error(FTSH_ERROR_FAILURE,line,"out of memory");
	}

	return s;
}

static char * expr_eval_access( struct expr *e, time_t stoptime )
{
	char *path;
	char *result;
	int ival;
	int mode;

	path = expr_eval(e->a,stoptime);
	if(!path) return 0;

	switch(e->type) {
		case EXPR_EXISTS:
			mode = F_OK;
			break;
		case EXPR_ISR:
			mode = R_OK;
			break;
		case EXPR_ISW:
			mode = W_OK;
			break;
		case EXPR_ISX:
			mode = X_OK;
			break;
		default:
			ftsh_fatal(e->line,"unexpected expression type %d",e->type);
			break;
	}

	ival = access(path,mode);
	if(ival!=0) {
		switch(errno) {
			#ifdef EACCES
			case EACCES:
			#endif

			#ifdef EROFS
			case EROFS:
			#endif

			#ifdef ENOENT
			case ENOENT:
			#endif

			#ifdef ENOTDIR
			case ENOTDIR:
			#endif

			#ifdef ELOOP
			case ELOOP:
			#endif
				result = xxstrdup("false");
				break;
			default:
				result = 0;
				break;
		}
	} else {
		result = xxstrdup("true");
	}

	if(result) {
		ftsh_error(FTSH_ERROR_COMMAND,e->line,"%s %s is %s",expr_type_to_string(e->type),path,result);
	} else {
		ftsh_error(FTSH_ERROR_FAILURE,e->line,"%s %s failed: %s",expr_type_to_string(e->type),path,strerror(errno));
	}
	free(path);
	return result;
}

static char * expr_eval_islink( struct expr *e, time_t stoptime )
{
	char buf[PATH_MAX];
	char *path;
	char *r;
	int result;

	path = expr_eval(e->a,stoptime);
	if(!path) return 0;

	result = readlink(path,buf,sizeof(buf));
	if(result>=0) {
		r = xxstrdup("true");
	} else switch(errno) {
		case EINVAL:
		case ENOENT:
		case ENOTDIR:
		case EISDIR:
		case EACCES:
		case ENAMETOOLONG:
			r = xxstrdup("false");
			break;
		default:
			r = 0;
			break;
	}

	free(path);
	return r;
}

static char * expr_eval_filetype( struct expr *e, time_t stoptime )
{
	char *path;
	char *result;
	struct stat buf;
	int ival;

	path = expr_eval(e->a,stoptime);
	if(!path) return 0;

	ival = stat(path,&buf);
	if(ival!=0) {
		switch(errno) {
			#ifdef ENOENT
			case ENOENT:
			#endif

			#ifdef ENOTDIR
			case ENOTDIR:
			#endif

			#ifdef ELOOP
			case ELOOP:
			#endif

			#ifdef EACCES
			case EACCES:
			#endif
				result = xxstrdup("false");
				break;

			default:
				result = 0;
				break;
		}
	} else {
		switch(e->type) {
			case EXPR_ISBLOCK:
				ival = S_ISBLK(buf.st_mode);
				break;
			case EXPR_ISCHAR:
				ival = S_ISCHR(buf.st_mode);
				break;
			case EXPR_ISDIR:
				ival = S_ISDIR(buf.st_mode);
				break;
			case EXPR_ISFILE:
				ival = S_ISREG(buf.st_mode);
				break;
			case EXPR_ISLINK:
				/* We should not get here because of shortcut earlier to avoid the broken S_ISLNK macro. */
				abort();
				ival = S_ISLNK(buf.st_mode);
				break;
			case EXPR_ISPIPE:
				ival = S_ISFIFO(buf.st_mode);
				break;
			case EXPR_ISSOCK:
				ival = S_ISSOCK(buf.st_mode);
				break;
			default:
				ftsh_fatal(e->line,"unexpected expression type %d",e->type);
				break;
		}

		if(ival) {
			result = xxstrdup("true");
		} else {
			result = xxstrdup("false");
		}
	}

	if(result) {
		ftsh_error(FTSH_ERROR_COMMAND,e->line,"%s %s is %s",expr_type_to_string(e->type),path,result);
	} else {
		ftsh_error(FTSH_ERROR_FAILURE,e->line,"%s %s failed: %s",expr_type_to_string(e->type),path,strerror(errno));
	}
	free(path);
	return result;
}

static char * expr_eval_range( struct expr *e, time_t stoptime )
{
	ftsh_integer_t i, ia, ib;
	char *r=0;

	if( expr_to_integer(e->a,&ia,stoptime) && expr_to_integer(e->b,&ib,stoptime) ) {

		ftsh_integer_t step;
		int digits;

		/* use the step if we have it */

		if(e->c) {
			if(!expr_to_integer(e->c,&step,stoptime)) return 0;
		} else {
			step = 1;
		}

		digits = MAX(digits_in_int(ia),digits_in_int(ib));
		/* add one for a unary - and for a space */
		digits+=2;

		/* allocate enough space for the set */
		r = xxmalloc((ABS((ib-ia)/step)+1)*digits+4);

		strcpy(r,"\"");

		/* fill up the text */
		if(ia<=ib) {
			for( i=ia; i<=ib; i+=ABS(step) ) {
				sprintf(&r[strlen(r)],"%ld ",i);
			}
		} else {
			for( i=ib; i<=ia; i+=ABS(step) ) {
				sprintf(&r[strlen(r)],"%ld ",i);
			}
		}
		strcat(r,"\"");
	}

	return r;
}

static char * expr_eval_fcall( struct expr *e, time_t stoptime )
{
	struct expr *f;
	char *name;
	char *rval=0;
	int i, argc=0;
	char **argv;

	name = ast_word_execute(e->line,e->literal);
	if(name) {
		argc = 1;
		for(f=e->a;f;f=f->next) argc++;
		argv = xxmalloc( sizeof(char*)*argc );
		for(i=0;i<argc;i++) argv[0] = 0;
		argv[0] = xxstrdup(name);
		f = e->a;
		for(i=1;i<argc;i++) {
			argv[i] = expr_eval(f,stoptime);
			if(!argv[i]) break;
			f = f->next;
		}
		if(i==argc) {
			rval = ast_function_execute(e->line,argc,argv,stoptime);
		}
		for(i=i-1;i>=0;i--) free(argv[i]);
		free(argv);
		free(name);
	}

	return rval;
}

char * expr_eval( struct expr *e, time_t stoptime )
{
	ftsh_integer_t i, ia, ib;
	ftsh_boolean_t b, ba, bb;
	char *r=0, *ra, *rb;

	switch(e->type) {
		case EXPR_ADD:
			if( expr_to_integer(e->a,&ia,stoptime) && expr_to_integer(e->b,&ib,stoptime) ){
				i = ia + ib;
				r = integer_to_string(e->line,i);
			}
			break;
		case EXPR_SUB:
			if( expr_to_integer(e->a,&ia,stoptime) && expr_to_integer(e->b,&ib,stoptime) ){
				i = ia - ib;
				r = integer_to_string(e->line,i);
			}
			break;
		case EXPR_MUL:
			if( expr_to_integer(e->a,&ia,stoptime) && expr_to_integer(e->b,&ib,stoptime) ){
				i = ia * ib;
				r = integer_to_string(e->line,i);
			}
			break;
		case EXPR_DIV:
			if( expr_to_integer(e->a,&ia,stoptime) && expr_to_integer(e->b,&ib,stoptime) ){
				i = ia / ib;
				r = integer_to_string(e->line,i);
			}
			break;
		case EXPR_MOD:
			if( expr_to_integer(e->a,&ia,stoptime) && expr_to_integer(e->b,&ib,stoptime) ){
				i = ia % ib;
				r = integer_to_string(e->line,i);
			}
			break;
		case EXPR_POW:
			if( expr_to_integer(e->a,&ia,stoptime) && expr_to_integer(e->b,&ib,stoptime) ){
				i=1;
				while( ib>0 ) {
					i = i*ia;
					ib--;
				}
				r = integer_to_string(e->line,i);
			}
			break;
		case EXPR_EQ:
			ra = expr_eval(e->a,stoptime);
			rb = expr_eval(e->b,stoptime);
			b = !strcmp(ra,rb);
			free(ra);
			free(rb);
			r = boolean_to_string(e->line,b);
			break;
		case EXPR_NE:
			ra = expr_eval(e->a,stoptime);
			rb = expr_eval(e->b,stoptime);
			b = strcmp(ra,rb);
			free(ra);
			free(rb);
			r = boolean_to_string(e->line,b);
			break;
		case EXPR_EQL:
			if( expr_to_integer(e->a,&ia,stoptime) && expr_to_integer(e->b,&ib,stoptime) ){
				b = (ia==ib);
				r = boolean_to_string(e->line,b);
			}
			break;
		case EXPR_NEQL:
			if( expr_to_integer(e->a,&ia,stoptime) && expr_to_integer(e->b,&ib,stoptime) ){
				b = (ia!=ib);
				r = boolean_to_string(e->line,b);
			}
			break;
		case EXPR_LT:
			if( expr_to_integer(e->a,&ia,stoptime) && expr_to_integer(e->b,&ib,stoptime) ){
				b = ia < ib;
				r = boolean_to_string(e->line,b);
			}
			break;
		case EXPR_LE:
			if( expr_to_integer(e->a,&ia,stoptime) && expr_to_integer(e->b,&ib,stoptime) ){
				b = ia <= ib;
				r = boolean_to_string(e->line,b);
			}
			break;
		case EXPR_GT:
			if( expr_to_integer(e->a,&ia,stoptime) && expr_to_integer(e->b,&ib,stoptime) ){
				b = ia > ib;
				r = boolean_to_string(e->line,b);
			}
			break;
		case EXPR_GE:
			if( expr_to_integer(e->a,&ia,stoptime) && expr_to_integer(e->b,&ib,stoptime) ){
				b = ia >= ib;
				r = boolean_to_string(e->line,b);
			}
			break;
		case EXPR_AND:
			if( expr_to_boolean(e->a,&ba,stoptime) && expr_to_boolean(e->b,&bb,stoptime) ){
				b = ba && bb;
				r = boolean_to_string(e->line,b);
			}
			break;
		case EXPR_OR:
			if( expr_to_boolean(e->a,&ba,stoptime) && expr_to_boolean(e->b,&bb,stoptime) ){
				b = ba || bb;
				r = boolean_to_string(e->line,b);
			}
			break;
		case EXPR_NOT:
			if(expr_to_boolean(e->a,&b,stoptime)) {
				r = boolean_to_string(e->line,!b);
			}
			break;
		case EXPR_EXISTS:
		case EXPR_ISR:
		case EXPR_ISW:
		case EXPR_ISX:
			r = expr_eval_access(e,stoptime);
			break;
		case EXPR_ISBLOCK:
		case EXPR_ISCHAR:
		case EXPR_ISDIR:
		case EXPR_ISFILE:
		case EXPR_ISPIPE:
		case EXPR_ISSOCK:
			r = expr_eval_filetype(e,stoptime);
			break;
		case EXPR_ISLINK:
			r = expr_eval_islink(e,stoptime);
			break;
		case EXPR_EXPR:
			r = expr_eval(e->a,stoptime);
			break;
		case EXPR_TO:
			r = expr_eval_range(e,stoptime);
			break;
		case EXPR_FCALL:
			r = expr_eval_fcall(e,stoptime);
			break;
		case EXPR_LITERAL:
			return ast_word_list_execute(e->line,e->literal);
			break;
	}

	return r;
}

void expr_print( FILE *file, struct expr *e )
{
	expr_print_list(file,e,0);
}

static void expr_print_list( FILE *file, struct expr *e, int with_commas )
{
	if(!e) return;

	switch(e->type) {
		case EXPR_ADD:
		case EXPR_SUB:
		case EXPR_MUL:
		case EXPR_DIV:
		case EXPR_MOD:
		case EXPR_POW:
		case EXPR_EQ:
		case EXPR_NE:
		case EXPR_EQL:
		case EXPR_NEQL:
		case EXPR_LT:
		case EXPR_LE:
		case EXPR_GT:
		case EXPR_GE:
		case EXPR_AND:
		case EXPR_OR:
			expr_print(file,e->a);
			fprintf(file," %s ",expr_type_to_string(e->type));
			expr_print(file,e->b);
			break;
		case EXPR_NOT:
		case EXPR_EXISTS:
		case EXPR_ISR:
		case EXPR_ISW:
		case EXPR_ISX:
		case EXPR_ISBLOCK:
		case EXPR_ISCHAR:
		case EXPR_ISDIR:
		case EXPR_ISFILE:
		case EXPR_ISLINK:
		case EXPR_ISPIPE:
		case EXPR_ISSOCK:
			fprintf(file,"%s ",expr_type_to_string(e->type));
			expr_print(file,e->a);
			break;
		case EXPR_TO:
			expr_print(file,e->a);
			fprintf(file," .to. ");
			expr_print(file,e->b);
			if(e->c) {
				fprintf(file," .step. ");
				expr_print(file,e->c);
			}
			fprintf(file," ");
			break;
		case EXPR_EXPR:
			fprintf(file,"(");
			expr_print(file,e->a);
			fprintf(file,")");
			break;
		case EXPR_FCALL:
			ast_word_print(file,e->literal);
			fprintf(file,"(");
			expr_print_list(file,e->a,1);
			fprintf(file,")");
			break;
		case EXPR_LITERAL:
			ast_word_print(file,e->literal);
			break;
		default:
			ftsh_fatal(e->line,"unknown expression type %d",e->type);
			break;
	}

	if(e->next) {
		if(with_commas) {
			fprintf(file,",");
		} else {
			fprintf(file," ");
		}
		expr_print(file,e->next);
	}
}

int expr_is_list( struct expr *e )
{
	if(e->type==EXPR_TO) {
		return 1;
	} else if(e->type==EXPR_EXPR) {
		return expr_is_list(e->a);
	} else {
		return 0;
	}
}

/* vim: set noexpandtab tabstop=4: */
