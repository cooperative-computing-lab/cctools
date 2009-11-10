/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PROCESS_H
#define PROCESS_H

#include <sys/wait.h>

/** @file process.h
Provides a higher level interface to finding information about complete processes.
Useful as a replacement for <tt>wait</tt>, <tt>waitpid</tt> and similar calls,
which do not allow the caller to check for completion without permanently removing
the completion notice.
<p>
Call @ref process_pending to see if there is a recently completed process,
@ref process_wait to wait for completion with a timeout, and @ref process_putback
to put the completion back into the queue.
*/

/** Describes a completed process.
Each element of this structure is the same as returned by Unix wait4().
@see process_wait
*/

struct process_info {
	pid_t         pid;    /**< The process ID of a complete process. */
	int           status; /**< The exit status of the process. */
	struct rusage rusage; /**< The resource usage of the process. */
};

/** Wait for a process to complete, and return its status.
Wait for up to timeout seconds for a child process to complete.
If a process has completed, its status will be returned in a @ref process_info
structure.  The caller may either call <tt>free</tt> to release the structure,
or may return it via @ref process_putback in order to allow another caller to retrieve it.
@param timeout The time, in seconds to wait for a child to complete.  If zero, do not wait at all.
@return A @ref process_info structure describing the child process status, or null if no
process completed in the available time.
*/

struct process_info * process_wait( int timeout );

/** Detect if a child process has completed.
If so, its status may be obtained without delay by calling @ref process_wait .
@return True if a child process has completed.
*/

int process_pending();

/** Return a process_info structure to the queue.
@param p A @ref process_info structure returned by @ref process_wait.
*/

void process_putback( struct process_info *p );



#endif
