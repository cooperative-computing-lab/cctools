/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef FAST_POPEN_H
#define FAST_POPEN_H

#include <stdio.h>

/** @file fast_popen.h Fast process invocation.*/

/** Fast process invocation.
@ref fast_popen opens a process for execution, providing its output
on a stream, just like the standard <tt>popen</tt>.  However, @ref fast_popen
does not invoke the shell the interpret the command, which can be very
time consuming.  Thus, the command must be given as a full path, and may
not include quotes, variables, or other features of the shell.
@param command The command string to execute.
@return A pointer to a file stream which must be closed with @ref fast_pclose.
*/
FILE *fast_popen(const char *command);

/** Conclude a fast process stream.
@param file A file pointer returned from @ref fast_popen.
@return The exit status of the process.
*/

int fast_pclose(FILE * file);

#endif
