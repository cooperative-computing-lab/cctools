/*
Copyright (C) 2011- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DPOPEN_H
#define DPOPEN_H

#include <unistd.h>
#include <stdio.h>

/** @file dpopen.h Double Pipe process invocation. */

/** FIXME Fast process invocation.
  * FIXME mention there is no pipe close, we rely on SIGPIPE to kill the child
@ref fast_popen opens a process for execution, providing its output
on a stream, just like the standard <tt>popen</tt>.  However, @ref fast_popen
does not invoke the shell the interpret the command, which can be very
time consuming.  Thus, the command must be given as a full path, and may
not include quotes, variables, or other features of the shell.
@param command The command string to execute.
@return A pointer to a file stream which must be closed with @ref fast_pclose.
*/
pid_t dpopen(const char *command, FILE ** in, FILE ** out);

int dpclose(FILE * in, FILE * out, pid_t pid);

#endif
