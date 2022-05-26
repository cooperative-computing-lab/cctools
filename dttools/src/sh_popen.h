/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


#ifndef SH_POPEN_H
#define SH_POPEN_H

#include <stdio.h>
#include "process.h"

/** @file sh_popen.h Non Terminal-Stealing popen implementation.*/

/** Non Terminal-Stealing popen
@ref sh_popen opens a process for execution, providing its output
on a stream, just like the standard <tt>popen</tt>.  However, @ref sh_popen
does not steal the terminal control away from the user, making ctrl-c slightly
more usuable when signal capturing. This happens by sh_popen forking a child
process.
@param command The command string to execute.
@return A pointer to a file stream which must be closed with @ref sh_pclose.
*/
FILE *sh_popen(char *command);


/** Conclude a sh_popen stream.
@param file A file pointer returned from @ref sh_popen.
@return The exit status of the process.
*/
int sh_pclose(FILE * file);


/** Non Terminal-Stealing system call
@ref sh_system opens a process for execution, and returns the exit status of
that call, just like the standard <tt>system</tt>.  However, @ref sh_system
does not steal the terminal control away from the user, making ctrl-c slightly
more usuable when signal capturing. This happens by sh_system forking a child
process.
@param command The command string to execute.
@return The exit status of the command just executed
*/
int sh_system(char* command);

#endif
