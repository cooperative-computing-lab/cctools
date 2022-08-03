/*
Copyright (C) 2019- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef WORK_QUEUE_JSON_H
#define WORK_QUEUE_JSON_H

/** @file work_queue_json.h A manager-worker library.
 The work queue provides an implementation of the manager-worker computing model
 using TCP sockets, Unix applications, and files as intermediate buffers.  A
 manager process uses @ref work_queue_json_create to create a queue, then @ref
 work_queue_json_submit to submit tasks. Once tasks are running, call @ref
 work_queue_json_wait to wait for completion.
*/

#include "work_queue.h"

/** Create a new work_queue object.
@param str A json document with properties to configure a new queue. Allowed properties are port, name, and priority.
@return A new work queue, or null if it could not be created.
 */
struct work_queue *work_queue_json_create(const char *str);

/** Submit a task to a queue.
Once a task is submitted to a queue, it is not longer under the user's
control and should not be inspected until returned via @ref work_queue_wait.
Once returned, it is safe to re-submit the same take object via
@ref work_queue_submit.
@param q A work queue object.
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
int work_queue_json_submit(struct work_queue *q, const char *str);

/** Wait for a task to complete.
@param q A work queue object.
@param timeout The number of seconds to wait for a completed task before
returning. Use an integer time to set the timeout or the constant
@ref WORK_QUEUE_WAITFORTASK to block until a task has completed.
@return A JSON description of the completed task or the
 timeout was reached without a completed task, or there is completed child
process (call @ref process_wait to retrieve the status of the completed
child process). Return string should be freed using free().

{ "command_line" : <i>string</i> , "tag" : <i>string</i> , "output" : <i>string</i> , "taskid" :
<i>integer</i> , "return_status" : <i>integer</i> , "result" : <i>integer</i> }

*/
char *work_queue_json_wait(struct work_queue *q, int timeout);


/** Remove a task from the queue.
@param q A work queue object.
@param id The id of the task to be removed from the queue.
@return A JSON description of the removed task.
*/
char *work_queue_json_remove(struct work_queue *q, int id);

/** Get the status for a given work queue.
@param q A work queue object.
@return A JSON description of the stats of the given work queue object.
*/
char *work_queue_json_get_status(struct work_queue *q);

#endif
