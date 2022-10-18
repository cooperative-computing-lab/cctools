/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef TASKVINE_JSON_H
#define TASKVINE_JSON_H

/** @file taskvine_json.h Provides a higher-level JSON-oriented abstraction
on top of the standard C interface in @ref taskvine.h.

An application uses  @ref vine_json_create to create a manager,
then @ref vine_json_submit to submit tasks, and @ref vine_json_wait to
wait for completion.  Details of tasks and the manager are carried
in JSON details (which must be parsed) rather than in C structures.
This provides a starting point for building interfaces to languages
outside of C, without relying on SWIG.

This module is a work in progress and is not yet ready for production.
*/

struct vine_manager;

/** Create a new manager object.
@param str A json document with properties to configure a new manager. Allowed properties are port, name, and priority.
@return A new manager, or null if it could not be created.
 */
struct vine_manager *vine_json_create(const char *str);

/** Submit a task to a manager.
Once a task is submitted to a manager, it is not longer under the user's
control and should not be inspected until returned via @ref vine_wait.
Once returned, it is safe to re-submit the same take object via
@ref vine_submit.
@param m A manager object.
@param str A JSON description of a task.

task document: (only "command_line" is required.)
{
    "command_line" : <i>string</i>,
    "input_files"  : <i>array of objects with one object per input file (see file document below)</i>,
    "output_files" : <i>array of objects with one object per output file (see file document below)</i>,
    "environment"  : <i>object with environment variables names and values (see environment document below)</i>,
    "tag"          : <i>string</i>,  # arbitrary string to identify the task by the user.
}

file document:
{
    "local_name"  : <i>string</i>,   # name of the file at the machine running the manager
    "remote_name" : <i>string</i>,   # name of the file local_name is copied to/from the machine running the task.
    "flags"       : {
                        "cache" : <i>boolean</i>,  # whether the file should be cached at the worker. Default is false.
                        "watch" : <i>boolean</i>,  # For output files only. Whether appends to the file should be sent
                                                     as they occur. Default is false.
                    }
}

environment document:
{
    <i>string</i> : <i>string</i>,   # name and value of an environment variable to be set for the task.
    <i>string</i> : <i>string</i>,
    ...
}

@return An integer task_id assigned to the submitted task.
*/
int vine_json_submit(struct vine_manager *m, const char *str);

/** Wait for a task to complete.
@param m A manager object.
@param timeout The number of seconds to wait for a completed task before
returning. Use an integer time to set the timeout or the constant
@ref VINE_WAITFORTASK to block until a task has completed.
@return A JSON description of the completed task or the
 timeout was reached without a completed task, or there is completed child
process (call @ref process_wait to retrieve the status of the completed
child process). Return string should be freed using free().

{ "command_line" : <i>string</i> , "tag" : <i>string</i> , "output" : <i>string</i> , "task_id" :
<i>integer</i> , "return_status" : <i>integer</i> , "result" : <i>integer</i> }

*/
char *vine_json_wait(struct vine_manager *m, int timeout);

/** Determine whether the manager is 'hungry' for more tasks.
While a taskvine Manager can handle a very large number of tasks,
it runs most efficiently when the number of tasks is slightly
larger than the number of active workers.  This function gives
the user of a flexible application a hint about whether it would
be better to submit more tasks via @ref vine_submit or wait for some to complete
via @ref vine_wait.
@param m A vine_manager object
@returns The number of additional tasks that can be efficiently submitted,
or zero if the manager has enough to work with right now.
*/
int vine_json_empty( struct vine_manager *m );

/** Determine whether the manager is empty.
When all of the desired tasks have been submitted to the manager,
the user should continue to call @ref vine_wait until
this function returns true.
@param m A vine_manager object
@returns True if the manager is completely empty, false otherwise.
*/
int vine_json_hungry( struct vine_manager *m );


/** Remove a task from the manager.
@param m A manager object.
@param id The id of the task to be removed from the manager.
@return A JSON description of the removed task.
*/
char *vine_json_remove(struct vine_manager *m, int id );

/** Get the status for a given manager.
@param m A manager object.
@return A JSON description of the stats of the given manager object.
*/
char *vine_json_get_status(struct vine_manager *m );

/** Delete a vine_manager object.
@param m A manager object.
*/

void vine_json_delete( struct vine_manager *m );

#endif
