/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_JSON_H
#define DS_JSON_H

/** @file ds_json.h Provides a higher-level JSON-oriented abstraction
on top of the standard C interface in @ref dataswarm.h.

An application uses  @ref ds_json_create to create a manager,
then @ref ds_json_submit to submit tasks, and @ref ds_json_wait to
wait for completion.  Details of tasks and the manager are carried
in JSON details (which must be parsed) rather than in C structures.

This module is used as the basis for building interfaces to
dynamic languages outside of C.
*/

struct ds_manager;

/** Create a new work_queue object.
@param str A json document with properties to configure a new queue. Allowed properties are port, name, and priority.
@return A new manager, or null if it could not be created.
 */
struct ds_manager *ds_json_create(const char *str);

/** Submit a task to a queue.
Once a task is submitted to a queue, it is not longer under the user's
control and should not be inspected until returned via @ref ds_wait.
Once returned, it is safe to re-submit the same take object via
@ref ds_submit.
@param q A manager object.
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

@return An integer taskid assigned to the submitted task.
*/
int ds_json_submit(struct ds_manager *q, const char *str);

/** Wait for a task to complete.
@param q A manager object.
@param timeout The number of seconds to wait for a completed task before
returning. Use an integer time to set the timeout or the constant
@ref DS_WAITFORTASK to block until a task has completed.
@return A JSON description of the completed task or the
 timeout was reached without a completed task, or there is completed child
process (call @ref process_wait to retrieve the status of the completed
child process). Return string should be freed using free().

{ "command_line" : <i>string</i> , "tag" : <i>string</i> , "output" : <i>string</i> , "taskid" :
<i>integer</i> , "return_status" : <i>integer</i> , "result" : <i>integer</i> }

*/
char *ds_json_wait(struct ds_manager *q, int timeout);

/** Determine whether the manager is 'hungry' for more tasks.
While the Data Swarm can handle a very large number of tasks,
it runs most efficiently when the number of tasks is slightly
larger than the number of active workers.  This function gives
the user of a flexible application a hint about whether it would
be better to submit more tasks via @ref ds_submit or wait for some to complete
via @ref ds_wait.
@param q A ds_manager object
@returns The number of additional tasks that can be efficiently submitted,
or zero if the manager has enough to work with right now.
*/
int ds_json_empty( struct ds_manager *q );

/** Determine whether the manager is empty.
When all of the desired tasks have been submitted to the manager,
the user should continue to call @ref ds_wait until
this function returns true.
@param q A ds_manager object
@returns True if the manager is completely empty, false otherwise.
*/
int ds_json_hungry( struct ds_manager *q );


/** Remove a task from the queue.
@param q A manager object.
@param id The id of the task to be removed from the queue.
@return A JSON description of the removed task.
*/
char *ds_json_remove(struct ds_manager *q, int id);

/** Get the status for a given manager.
@param q A manager object.
@return A JSON description of the stats of the given manager object.
*/
char *ds_json_get_status(struct ds_manager *q);

void ds_json_delete( struct ds_manager *q );

#endif
