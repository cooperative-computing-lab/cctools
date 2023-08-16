/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"

#include "full_io.h"

#include <unistd.h>

#include <string.h>

void debug_stderr_write (INT64_T flags, const char *str)
{
	full_write(STDERR_FILENO, str, strlen(str));
}

void debug_stdout_write (INT64_T flags, const char *str)
{
	full_write(STDOUT_FILENO, str, strlen(str));
}

/* vim: set noexpandtab tabstop=8: */
