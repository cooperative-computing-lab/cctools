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

/** Fast process invocation.
@ref fast_popen opens a process for execution, providing its output
on a stream, just like the standard <tt>popen</tt>.  However, @ref fast_popen
does not invoke the shell to interpret the command, which can be very
time consuming.  Thus, the command must be given as a full path, and may
not include quotes, variables, or other features of the shell.
(Note: there is no pipe close, we rely on SIGPIPE to kill the child.)@param command The command string to execute.
@param in A pointer to a standard I/O stream, which this function will attach to the standard input of the process.
@param out A pointer to a standard I/O stream, which this function will attach to the standard input of the child process.
@return The process ID of the newly created process.
*/
pid_t dpopen(const char *command, FILE ** in, FILE ** out);

/** Conclude a fast process stream.
@param in The standard input stream returned from @ref dpopen.
@param out The standard input stream returned from @ref dpopen.
@param pid The process ID returned from @ref dpopen.
@return The exit status of the process.
*/
int dpclose(FILE * in, FILE * out, pid_t pid);

#endif
