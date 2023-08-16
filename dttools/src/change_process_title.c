/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef CCTOOLS_OPSYS_LINUX

#include "change_process_title.h"

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static char *process_title = 0;
static int process_title_length;

void change_process_title_init(char **argv)
{
	char *process_title_end;
	char **newargv;
	int i, argc;

	/* count up the arguments */
	for(argc = 0; argv[argc]; argc++) {
	}

	/* duplicate the entire argv array */
	newargv = malloc(argc * sizeof(char *));
	for(i = 0; i < argc; i++) {
		newargv[i] = strdup(argv[i]);
	}

	/* save the space where we were operating */
	process_title = argv[0];
#ifdef _GNU_SOURCE
	{
		extern char *program_invocation_name;
		extern char *program_invocation_short_name;
		program_invocation_name = argv[0];
		program_invocation_short_name = argv[0];
	}
#endif
	process_title_end = argv[argc - 1] + strlen(argv[argc - 1]);
	process_title_length = process_title_end - process_title;

	/* reload argv with the copied values */
	for(i = 0; i < argc; i++) {
		argv[i] = newargv[i];
	}

	free(newargv);
}

void change_process_title(const char *fmt, ...)
{
	int length, i;
	va_list args;
	va_start(args, fmt);

	if(!process_title)
		return;

	/* print the new process title in place */
	length = vsnprintf(process_title, process_title_length, fmt, args);

	/* null out the rest of the string */
	for(i = length; i < process_title_length; i++) {
		process_title[i] = 0;
	}

	/*
	   Note that we have to change the final char to
	   indicate that the title has been changed,
	   to avoid screwing up the output of ps.
	 */

	process_title[i - 1] = 0;
	process_title[i] = 'x';

	va_end(args);
}

#else

/*
Changing the process title is not supported
on any other platform.
*/

void change_process_title_init(char **argv)
{
}

void change_process_title(char *fmt, ...)
{
}

#endif

/* vim: set noexpandtab tabstop=8: */
