/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ast.h"
#include "ast_execute.h"
#include "buffer.h"
#include "ftsh_error.h"
#include "expr.h"
#include "timed_exec.h"
#include "multi_fork.h"
#include "glob.h"
#include "builtin.h"

#include "macros.h"
#include "sleeptools.h"
#include "xxmalloc.h"
#include "variable.h"
#include "stringtools.h"
#include "hash_table.h"

#include <time.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <signal.h>
#include <limits.h>

#ifndef LINE_MAX
#define LINE_MAX 4096
#endif

static char * ast_expr_list_execute( int linenum, struct expr *e, time_t stoptime );
static char * ast_bareword_execute( int linenum, char *line );
static int ast_do_simple( int line, int argc, char **argv, int fds[3], time_t stoptime );
static int process_status( const char *name, pid_t pid, int status, int line );

extern int ftsh_expmin, ftsh_expmax, ftsh_expfactor, ftsh_exprand;

/*
ftable maps function names to the ast_group that implements it.
*/

static struct hash_table *ftable = 0;

int ast_program_execute( struct ast_group *program, time_t stoptime )
{
	struct ast_group *g;
	struct ast_function *f, *old;

	/*
	First, fill up the function table with all of the functions
	in this entire syntax tree.
	*/

	ftable = hash_table_create(127,hash_string);
	if(!ftable) ftsh_fatal(0,"out of memory");

	for( g=program; g; g=g->next ) {
		if(g->command->type==AST_COMMAND_FUNCTION) {
			f = g->command->u.function;
			old = hash_table_remove(ftable,f->name->text);
			if(old) {
				ftsh_error(FTSH_ERROR_SYNTAX,f->function_line,"function %s is defined twice (first at line %d)",f->name->text,old->function_line);
				return 0;
			}
			if(!hash_table_insert(ftable,f->name->text,f)) {
				ftsh_fatal(f->function_line,"out of memory");
			}
		}
	}

	return ast_group_execute(program,stoptime);
}

/*
ast_function_execute runs a function in an expression context,
while ast_group_execute runs the same code in a procedural context.
*/

char * ast_function_execute( int line, int argc, char **argv, time_t stoptime )
{
	struct ast_function *f;
	char *rval = 0;

	if(variable_frame_push(line,argc,argv)) {
		f = hash_table_lookup(ftable,argv[0]);
		if(f) {
			if(ast_group_execute(f->body,stoptime)) {
				if(variable_rval_get()) {
					rval = xxstrdup(variable_rval_get());
					ftsh_error(FTSH_ERROR_STRUCTURE,line,"function %s returns %s",argv[0],rval);
				} else {
					ftsh_error(FTSH_ERROR_FAILURE,line,"function %s did not return a value",argv[0]);
				}
			}
		} else {
			ftsh_error(FTSH_ERROR_FAILURE,line,"function %s is not defined",argv[0]);
		}
		variable_frame_pop();
	}

	return rval;
}


int ast_group_execute( struct ast_group *g, time_t stoptime )
{
	int result;

	while(g && !variable_rval_get() ) {
		result = ast_command_execute(g->command,stoptime);
		if(!result) return 0;
		g = g->next;
	}
	return 1;
}

int ast_command_execute( struct ast_command *s, time_t stoptime )
{
	if( stoptime && time(0)>stoptime ) return 0;

	switch(s->type) {
		case AST_COMMAND_FUNCTION:
			return 1;
		case AST_COMMAND_CONDITIONAL:
			return ast_conditional_execute(s->u.conditional,stoptime);
		case AST_COMMAND_TRY:
			return ast_try_execute(s->u.try,stoptime);
		case AST_COMMAND_FORLOOP:
			return ast_forloop_execute(s->u.forloop,stoptime);
		case AST_COMMAND_WHILELOOP:
			return ast_whileloop_execute(s->u.whileloop,stoptime);
		case AST_COMMAND_SHIFT:
			return ast_shift_execute(s->u.shift,stoptime);
		case AST_COMMAND_RETURN:
			return ast_return_execute(s->u.rtn,stoptime);
		case AST_COMMAND_ASSIGN:
			return ast_assign_execute(s->u.assign,stoptime);
		case AST_COMMAND_SIMPLE:
			return ast_simple_execute(s->u.simple,stoptime);
		case AST_COMMAND_EMPTY:
			return 1;
		default:
			return 0;
	}
}

int ast_conditional_execute( struct ast_conditional *c, time_t stoptime )
{
	int result;
	ftsh_boolean_t b;

	ftsh_error(FTSH_ERROR_STRUCTURE,c->if_line,"IF");

	if(expr_to_boolean(c->expr,&b,stoptime)) {
		if(b) {
			result = ast_group_execute(c->positive,stoptime);
		} else {
			result = ast_group_execute(c->negative,stoptime);
		}
	} else {
		result = 0;
	}

	ftsh_error(FTSH_ERROR_STRUCTURE,c->end_line,"END");

	return result;
}

static int ast_try_body_execute( struct ast_try *t, time_t stoptime )
{
	int i=0;
	int result=0;
	ftsh_integer_t loops=0;
	int interval = ftsh_expmin;
	int sleeptime;
	time_t starttime, every;

	if(t->time_limit) {
		ftsh_integer_t timeout;
		if(expr_to_integer(t->time_limit->expr,&timeout,stoptime)) {
			timeout *= t->time_limit->units;
			if(stoptime==0) {
				stoptime = timeout+time(0);
			} else {
				stoptime = MIN(stoptime,timeout+time(0));
			}
		} else {
			return 0;
		}
	}

	if(t->every_limit) {
		ftsh_integer_t i;
		if(expr_to_integer(t->every_limit->expr,&i,stoptime)) {
			every = i*t->every_limit->units;
		} else {
			return 0;
		}
	} else {
		every = 0;
	}

	if(t->loop_limit) {
		if(expr_to_integer(t->loop_limit->expr,&loops,stoptime)) {
			/* no problem */
		} else {
			return 0;
		}
	}

	if(!t->time_limit && ! t->loop_limit) {
		loops = 1;
	}

	while(1) {
		ftsh_error(FTSH_ERROR_STRUCTURE,t->try_line,"TRY attempt %d",i);

		starttime = time(0);

		if(ast_group_execute(t->body,stoptime)) {
			result = 1;
			break;
		}

		i++;

		if( stoptime && (time(0) > stoptime) ) {
			ftsh_error(FTSH_ERROR_FAILURE,t->try_line,"TRY time expired");
			result = 0;
			break;
		}
	
		if(loops && (i>=loops)) {
			ftsh_error(FTSH_ERROR_FAILURE,t->try_line,"TRY loop limit reached");
			result = 0;
			break;
		}

		if(every) {
			ftsh_error(FTSH_ERROR_STRUCTURE,t->end_line,"TRY restricted to EVERY %s seconds",every);
			sleeptime = starttime+every-time(0);
			if(sleeptime<0) sleeptime = 0;
			ftsh_error(FTSH_ERROR_STRUCTURE,t->end_line,"TRY sleeping for %d seconds",sleeptime);
		} else {
			if(ftsh_exprand) {
				sleeptime = interval*(1 + 1.0*rand()/RAND_MAX);
			} else {
		       		sleeptime = interval;
			}
			ftsh_error(FTSH_ERROR_STRUCTURE,t->end_line,"TRY sleeping for %d seconds (base %d)",sleeptime,interval);
			interval = MIN(interval*ftsh_expfactor,ftsh_expmax);
		}

		sleep_for(sleeptime);
	}


	return result;
}

int ast_try_execute( struct ast_try *t, time_t stoptime )
{
	int result = ast_try_body_execute(t,stoptime);
	if(!result && t->catch_block ) {
		ftsh_error(FTSH_ERROR_STRUCTURE,t->catch_line,"CATCH");
		result = ast_group_execute(t->catch_block,stoptime);
	}

	ftsh_error(FTSH_ERROR_STRUCTURE,t->end_line,"END");

	return result;
}

int ast_whileloop_execute( struct ast_whileloop *w, time_t stoptime )
{
	int result=1;
	ftsh_boolean_t b;

	while(1) {
		ftsh_error(FTSH_ERROR_STRUCTURE,w->while_line,"WHILE");

		if(expr_to_boolean(w->expr,&b,stoptime)) {
			if(b) {
				ftsh_error(FTSH_ERROR_STRUCTURE,w->while_line,"WHILE expression is true");

				if(ast_group_execute(w->body,stoptime)) {
					continue;
				} else {
					result = 0;
					break;
				}
			} else {
				ftsh_error(FTSH_ERROR_STRUCTURE,w->while_line,"WHILE expression is false");
				result = 1;
				break;
			}
		} else {
			ftsh_error(FTSH_ERROR_STRUCTURE,w->while_line,"WHILE expression failed");
			result = 0;
			break;
		}
	}

	ftsh_error(FTSH_ERROR_STRUCTURE,w->end_line,"END");

	return result;
}

static int ast_for_execute( struct ast_forloop *f, time_t stoptime, const char *name, int argc, char **argv )
{
	int i=0;
	int result=0;
	for(i=0;i<argc;i++) {
		ftsh_error(FTSH_ERROR_STRUCTURE,f->for_line,"%s=%s",name,argv[i]);
		result = buffer_save(name,argv[i]);
		if(!result) break;
		result = ast_group_execute(f->body,stoptime);
		if(!result) break;
	}

	return result;
}

static int ast_forany_execute( struct ast_forloop *f, time_t stoptime, const char *name, int argc, char **argv )
{
	int result=0;
	int start = rand()%argc;
	int i = start;

	while(1) {
		ftsh_error(FTSH_ERROR_STRUCTURE,f->for_line,"%s=%s",name,argv[i]);
		result = buffer_save(name,argv[i]);
		if(result) {
			result = ast_group_execute(f->body,stoptime);
			if(result) break;
		}
		i++;
		if(i>=argc) i=0;
		if(i==start) {
			result = 0;
			break;
		}
	}

	return result;
}

static int ast_forall_execute( struct ast_forloop *f, time_t stoptime, const char *name, int argc, char **argv )
{
	int i;
	int pid;
	int result;
	struct multi_fork_status *s;

	s = xxmalloc(sizeof(*s)*argc);

	pid = multi_fork(argc,s,stoptime,f->for_line);
	if(pid>=0) {
		srand(getpid());
		if(stoptime && (time(0)>stoptime)) _exit(1);

		ftsh_error(FTSH_ERROR_STRUCTURE,f->for_line,"%s=%s starting",name,argv[pid]);
		result = buffer_save(name,argv[pid]);
		if(!result) _exit(1);

		result = ast_group_execute(f->body,stoptime);
		if(result) {
			_exit(0);
		} else {
			_exit(1);
		}
	} else {
		for(i=0;i<argc;i++) {
			char str[LINE_MAX];
			if(s[i].state==MULTI_FORK_STATE_GRAVE) {
				snprintf(str,sizeof(str),"%s=%s",name,argv[i]);
				process_status(str,s[i].pid,s[i].status,f->for_line);
			}
		}

		free(s);

		if(pid==MULTI_FORK_SUCCESS) {
			return 1;
		} else {
			return 0;
		}
	}
}

int ast_forloop_execute( struct ast_forloop *f, time_t stoptime )
{
	char *loopname;
	char *name;
	char *line;
	int result=1;

	switch(f->type) {
		case AST_FOR:
			loopname = "FOR";
			break;
		case AST_FORALL:
			loopname = "FORALL";
			break;
		case AST_FORANY:
			loopname = "FORANY";
			break;
	}

	ftsh_error(FTSH_ERROR_STRUCTURE,f->for_line,"%s %s",loopname,f->name->text);

	name = ast_word_execute(f->for_line,f->name);
	if(name) {
		line = ast_expr_list_execute(f->for_line,f->list,stoptime);
		if(line) {
			int argc;
			char **argv;
			if(string_split_quotes( line, &argc, &argv )) {
				switch(f->type) {
					case AST_FOR:
						result = ast_for_execute(f,stoptime,name,argc,argv);
						break;
					case AST_FORANY:
						result = ast_forany_execute(f,stoptime,name,argc,argv);
						break;
					case AST_FORALL:
						result = ast_forall_execute(f,stoptime,name,argc,argv);
						break;
				}
				free(argv);
			} else {
				ftsh_error(FTSH_ERROR_FAILURE,f->for_line,"out of memory!");
				result = 0;
			}
			free(line);		
		}
		free(name);
	} else {
		result = 0;
	}

	ftsh_error(FTSH_ERROR_STRUCTURE,f->end_line,"END");
	return result;
}

int ast_assign_execute( struct ast_assign *a, time_t stoptime )
{
	int result;

	if(a->expr) {
		char *value;
		char *word;

		value = expr_eval(a->expr,stoptime);
		if(value) {
			word = ast_bareword_execute(a->line,value);
			if(word) {
				ftsh_error(FTSH_ERROR_COMMAND,a->line,"%s=%s",a->name->text,word);
				if(buffer_save(a->name->text,word)) {
					result=1;
				} else {
					ftsh_error(FTSH_ERROR_FAILURE,a->line,"couldn't store variable '%s': %s",a->name->text,strerror(errno));
					result=0;
				}
				free(word);
			} else {
				result = 0;
			}
			free(value);
		} else {
			result=0;
		}
	} else {
		ftsh_error(FTSH_ERROR_COMMAND,a->line,"%s=",a->name->text);
		buffer_delete(a->name->text);
		result = 1;
	}

	return result;
}

int ast_shift_execute( struct ast_shift *s, time_t stoptime )
{
	ftsh_integer_t value;
	
	if(s->expr) {
		if(!expr_to_integer(s->expr,&value,stoptime)) {
			return 0;
		}
	} else {
		value = 1;
	}

	return variable_shift(value,s->line);
}

int ast_return_execute( struct ast_return *s, time_t stoptime )
{
	char *value;

	value = expr_eval(s->expr,stoptime);
	if(value) {
		ftsh_error(FTSH_ERROR_STRUCTURE,s->line,"return value is %s",value);
		variable_rval_set(value);
		return 1;
	} else {
		ftsh_error(FTSH_ERROR_FAILURE,s->line,"couldn't compute return value");
		return 0;
	}
}

static int ast_redirect_open( struct ast_redirect *r, int line, int fds[3] )
{
	int fd;
	char *target;

	if(!r) return 1;

	target = ast_word_execute( line, r->target );
	if(!target) return 0;

	switch(r->kind) {
		case AST_REDIRECT_FILE:
			switch(r->mode) {
				case AST_REDIRECT_INPUT:
					fd = open(target,O_RDONLY);
					break;
				case AST_REDIRECT_OUTPUT:
					fd = open(target,O_WRONLY|O_CREAT|O_TRUNC,0777);
					break;
				case AST_REDIRECT_APPEND:
					fd = open(target,O_WRONLY|O_CREAT|O_APPEND,0777);
					break;
			}
			break;
		case AST_REDIRECT_BUFFER:
			switch(r->mode) {
				case AST_REDIRECT_INPUT:
					fd = buffer_open_input(target);
					break;
				case AST_REDIRECT_OUTPUT:	
					fd = buffer_open_output(target);
					break;
				case AST_REDIRECT_APPEND:
					fd = buffer_open_append(target);
					break;
			}
			break;
		case AST_REDIRECT_FD:
			fd = fds[atoi(target)];
			break;
	}

	if(fd<0) {
		ftsh_error(FTSH_ERROR_FAILURE,line,"couldn't redirect fd %d to %s: %s",r->source,target,strerror(errno));
		free(target);
		return 0;
	} else {
		r->actual = fd;
		fds[r->source] = fd;
		if(r->next) {
			return ast_redirect_open(r->next,line,fds);
		} else {
			return 1;
		}
	}
}

static void ast_redirect_close( struct ast_redirect *r )
{
	if(r) {
		switch(r->kind) {
			case AST_REDIRECT_FILE:
				if(r->actual>=0) {
					close(r->actual);
					r->actual = -1;
				}
				break;
			case AST_REDIRECT_FD:
			case AST_REDIRECT_BUFFER:
				/* Don't close buffers! */
				break;
		}
		ast_redirect_close(r->next);
	}
}

int ast_simple_execute( struct ast_simple *s, time_t stoptime )
{
	int result=0;
	int fds[3];
	int argc;
	char **argv;
	char *line;

	fds[0] = 0;
	fds[1] = 1;
	fds[2] = 2;

	if( stoptime && (time(0)>stoptime) ) return 0;

	if(ast_redirect_open(s->redirects,s->line,fds)) {
		line = ast_word_list_execute(s->line,s->words);
		if(line) {
			if(string_split_quotes(line,&argc,&argv)) {
				result = ast_do_simple(s->line,argc,argv,fds,stoptime);
				free(argv);
			}
			free(line);
		}
		ast_redirect_close(s->redirects);
	}

	return result;
}

/*
To evaluate an expression list, eval each of the sub expressions
and concat the results into one big string.
XXX hack: for list expressions, remove the quotes.
*/

static char * ast_expr_list_execute( int linenum, struct expr *e, time_t stoptime )
{
	char *line=0;
	char *v;

	while(e) {
		v = expr_eval(e,stoptime);
		if(v) {
			if(expr_is_list(e)) {
				int length = strlen(v);
				memcpy(v,&v[1],length-2);
				v[length-2] = 0;
			}
			if(line) {
				line = string_combine_multi(line,xxstrdup(" "),v,0);
			} else {
				line = v;
			}
			e=e->next;
			continue;
		} else {
			if(line) {
				free(line);
				line = 0;
			}
			break;
		}
	}

	return line;
}

/*
To build a word list, first glob each of the individual elements,
concat together all of the raw text with spaces in between,
then substitute variables and pass the line back.
It must then be re-split with string_split_quotes.
*/
#ifdef GLOB_NOMAGIC
# define GLOB_FLAGS GLOB_NOMAGIC
#else
# define GLOB_FLAGS GLOB_NOCHECK
#endif

char * ast_word_list_execute( int linenum, struct ast_word *w )
{
	char *t, *line = 0;
	int i, len;
	glob_t g;

	while( w ) {
/*
		This isn't correct.
		We need more thought on how to handle wildcards.
		if(strpbrk(w->text,"*[")) {
*/
		if(0) {
			if(glob(w->text,GLOB_FLAGS,0,&g)==0) {
				len=1;
				for(i=0;i<g.gl_pathc;i++) {
					len += strlen(g.gl_pathv[i])+1;
				}
				t = xxmalloc(len);
				t[0]=0;
				for(i=0;i<g.gl_pathc;i++) {
					strcat(t,g.gl_pathv[i]);
					strcat(t," ");
				}
				globfree(&g);
			} else {
				ftsh_error(FTSH_ERROR_FAILURE,linenum,"couldn't expand pattern %s",w->text);
				if(line) free(line);
				return 0;
			}
		} else {
			t=xxstrdup(w->text);
		}

		if(line) {
			line = string_combine_multi( line, xxstrdup(" "), t, 0 );
		} else {
			line = t;
		}

		w = w->next;
	}

	return variable_subst(line,linenum);
}

/*
When a single word is required, build a word list,
split it, and then assert that only one argument
be in the result.
*/

char * ast_word_execute( int linenum, struct ast_word *w )
{
	char *line;
	char *result=0;

	line = ast_word_list_execute( linenum, w );
	if(line) {
		result = ast_bareword_execute(linenum,line);
		free(line);
	}

	return result;
}

/*
Given a line, possibly containing multiple words and quotes,
determine if it is really and single word.  If so, return that.
*/

static char * ast_bareword_execute( int linenum, char *line )
{
	int argc;
	char **argv;
	char *result=0;

	if(string_split_quotes(line,&argc,&argv)) {
		if(argc==1) {
			result = xxstrdup(argv[0]);
		} else if(argc>1) {
			ftsh_error(FTSH_ERROR_SYNTAX,linenum,"expected only one word here, but got garbage following '%s'",argv[0]);
		} else {
			ftsh_error(FTSH_ERROR_SYNTAX,linenum,"expected a word here, but found nothing");
		}
		free(argv);
	}

	return result;
}

static int ast_do_internal( int line, int argc, char **argv, int fds[3], time_t stoptime )
{
	struct ast_function *f;
	builtin_func_t b;
	int oldfds[3];
	int result=0;

	f = hash_table_lookup(ftable,argv[0]);
	b = builtin_lookup(argv[0]);

	if(f) ftsh_error(FTSH_ERROR_STRUCTURE,f->function_line,"FUNCTION %s",f->name->text);

	if(b || variable_frame_push(f->function_line,argc,argv)) {

		if(fds[0]!=0) {
			oldfds[0] = dup(0);
			if(oldfds[0]<0) ftsh_fatal(line,"out of file descriptors");
			dup2(fds[0],0);
		}

		if(fds[1]!=1) {
			oldfds[1] = dup(1);
			if(oldfds[1]<0) ftsh_fatal(line,"out of file descriptors");
			dup2(fds[1],1);
		}

		if(fds[2]!=2) {
			oldfds[2] = dup(2);
			if(oldfds[2]<0) ftsh_fatal(line,"out of file descriptors");
			dup2(fds[2],2);
		}

		if(f) {
			result = ast_group_execute(f->body,stoptime);
		} else {
			result = b(line,argc,argv,stoptime);
		}

		if(fds[2]!=2) {
			dup2(oldfds[2],2);
			close(oldfds[2]);
		}

		if(fds[1]!=1) {
			dup2(oldfds[1],1);
			close(oldfds[1]);
		}

		if(fds[0]!=0) {
			dup2(oldfds[0],0);
			close(oldfds[0]);
		}

		if(f) variable_frame_pop();
	}

	if(f) ftsh_error(FTSH_ERROR_STRUCTURE,f->end_line,"END");

	return result;
}

static int ast_do_external( int line, int argc, char **argv, int fds[3], time_t stoptime )
{
	timed_exec_t tresult;
	int status;
	int result;
	pid_t pid;

	tresult = timed_exec(line,argv[0],argv,fds,&pid,&status,stoptime);
	if(tresult==TIMED_EXEC_TIMEOUT) {
		ftsh_error(FTSH_ERROR_FAILURE,line,"%s [%d] ran out of time",argv[0],pid);
		result = 0;
	} else if(tresult==TIMED_EXEC_NOEXEC) {
		ftsh_error(FTSH_ERROR_FAILURE,line,"%s [%d] couldn't be executed: %s",argv[0],pid,strerror(errno));
		result = 0;
	} else {
		result = process_status(argv[0],pid,status,line);
	}

	return result;
}

static int ast_do_simple( int line, int argc, char **argv, int fds[3], time_t stoptime )
{
	char *cmd;
	int length=0;
	int i;

	for( i=0; i<argc; i++ ) {
		length+=strlen(argv[i])+1;
	}

	cmd = xxmalloc(length+1);
	cmd[0] = 0;

	for( i=0; i<argc; i++ ) {
		strcat(cmd,argv[i]);
		strcat(cmd," ");
	}

	ftsh_error(FTSH_ERROR_COMMAND,line,"%s",cmd);

	free(cmd);

	if(hash_table_lookup(ftable,argv[0]) || builtin_lookup(argv[0]) ) {
		return ast_do_internal(line,argc,argv,fds,stoptime);
	} else {
		return ast_do_external(line,argc,argv,fds,stoptime);
	}
}

/*
Given a process status code, log the reasons for the exit,
and return true iff it completed normally with code zero.
*/

static int process_status( const char *name, pid_t pid, int status, int line )
{
	int result=0;

	if(WIFEXITED(status)) {
		int code = WEXITSTATUS(status);
		if(code==0) {
			ftsh_error(FTSH_ERROR_PROCESS,line,"%s [%d] exited normally with status %d",name,pid,code);
			result = 1;
		} else {
			ftsh_error(FTSH_ERROR_FAILURE,line,"%s [%d] exited normally status with %d",name,pid,code);
			result = 0;
		}
	} else if(WIFSIGNALED(status)) {
		int sig = WSTOPSIG(status);
		ftsh_error(FTSH_ERROR_FAILURE,line,"%s [%d] exited abnormally with signal %d (%s)",name,pid,sig,string_signal(sig));
		result = 0;
	} else {
		ftsh_error(FTSH_ERROR_FAILURE,line,"%s [%d] exited for unknown reasons (wait status %d)",name,pid,status);
		result = 0;
	}

	return result;
}

