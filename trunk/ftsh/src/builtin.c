/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "builtin.h"
#include "ftsh_error.h"
#include "buffer.h"
#include "xxmalloc.h"

#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <pwd.h>
#include <stdlib.h>

static int builtin_cd( int line, int argc, char **argv, time_t stoptime )
{
	int result;
	char *dirname;

	if(argc==2) {
		dirname = argv[1];
	} else if(argc==1) {
		struct passwd *p;
		p = getpwuid(getuid());
		if(p) {
			dirname = p->pw_dir;
		} else {
			ftsh_error(FTSH_ERROR_FAILURE,line,"cd: couldn't determine your home directory");
			dirname = 0;
		}
	} else {
		ftsh_error(FTSH_ERROR_SYNTAX,line,"cd: too many arguments");
		result = 0;
	}

	if(dirname) {
		ftsh_error(FTSH_ERROR_COMMAND,line,"CD %s",dirname);
		if(chdir(dirname)==0) {
			result = 1;
		} else {
			ftsh_error(FTSH_ERROR_FAILURE,line,"cd: couldn't move to %s: %s",dirname,strerror(errno));
			result = 0;
		}
	} else {
		result = 0;
	}

	return result;
}

static int builtin_export( int line, int argc, char **argv, time_t stoptime )
{
	char *expr;
	char *name;
	char *value;

	if(argc<2) {
		ftsh_error(FTSH_ERROR_SYNTAX,line,"export: exactly one argument needed");
		return 0;
	} else if(argc>2) {
		ftsh_error(FTSH_ERROR_SYNTAX,line,"export: too many arguments");
		return 0;
	}

	name = argv[1];

	value = buffer_load(name);
	if(!value) value = xxstrdup("");

	expr = malloc(strlen(name)+strlen(value)+3);
	if(!expr) {
		free(name);
		free(value);
		return 0;
	}

	ftsh_error(FTSH_ERROR_COMMAND,line,"EXPORT %s (%s)",name,value);

	sprintf(expr,"%s=%s",name,value);

	/* Depending on the libc, this call may leak memory */
	/* by leaving multiple exprs allocated.  No solution */
	/* except to leak.  Don't export in an infinite loop. */

	putenv(expr);

	free(name);
	free(value);

	return 1;
}

static int builtin_echo( int line, int argc, char **argv, time_t stoptime )
{
	int i;
	int do_newline = 1;

	for(i=1;i<argc;i++) {
		if(!strcmp(argv[i],"-n")) {
			do_newline = 0;
			continue;
		} else {
			printf("%s",argv[i]);
			if(i!=(argc-1)) printf(" ");
		}
	}

	if(do_newline) printf("\n");
	fflush(stdout);

	return 1;
}

static int builtin_exec( int line, int argc, char **argv, time_t stoptime )
{
	execvp(argv[1],&argv[1]);
	ftsh_error(FTSH_ERROR_FAILURE,line,"exec: %s failed: %s",argv[1],strerror(errno));
	return 0;
}

static int builtin_exit( int line, int argc, char **argv, time_t stoptime )
{
	int value;

	if(argc<2) {
		value = 0;
	} else {
		if(sscanf(argv[1],"%d",&value)!=1) {
			value = 1;
		}
	}

	ftsh_error(FTSH_ERROR_STRUCTURE,line,"exit: exiting with status %d",value);
	exit(value);
}

static int builtin_success( int line, int argc, char **argv, time_t stoptime )
{
	return 1;
}

static int builtin_failure( int line, int argc, char **argv, time_t stoptime )
{
	return 0;
}

struct builtin {
	const char *name;
	builtin_func_t func;
};

static struct builtin table[] = {
	{ "cd", builtin_cd },
	{ "export", builtin_export },
	{ "echo", builtin_echo },
	{ "exec", builtin_exec },
	{ "exit", builtin_exit },
	{ "success", builtin_success },
	{ "failure", builtin_failure },
	{ 0,0 },
};

builtin_func_t builtin_lookup( const char *name )
{
	int i;

	for(i=0;table[i].name;i++) {
		if(!strcmp(table[i].name,name)) {
			return table[i].func;
		}
	}

	return 0;
}

