#ifndef WORK_QUEUE_JSON_H
#define WORK_QUEUE_JSON_H

/** @file work_queue_json.h A master-worker library.
 The work queue provides an implementation of the master-worker computing model
 using TCP sockets, Unix applications, and files as intermediate buffers.  A
 master process uses @ref work_queue_json_create to create a queue, then @ref
 work_queue_json_submit to submit tasks. Once tasks are running, call @ref
 work_queue_json_wait to wait for completion. 
*/

#include "work_queue.h"

/** Create a new work_queue object.
@param port The port number to listen on. If zero is specified, then the 
port stored in <b>WORK_QUEUE_PORT</b> is used if available. If it isn't, or 
-1 is specified, the first unused port between <b>WORK_QUEUE_LOW_PORT</b> 
and <b>WORK_QUEUE_HIGH_PORT</b> (1024 and 32767 by default) is chosen.
@return A new work queue, or null if it could not be created. 
 */
struct work_queue* work_queue_json_create(const char* str);

/** Submit a task to a queue.
Once a task is submitted to a queue, it is not longer under the user's 
control and should not be inspected until returned via @ref work_queue_wait.
Once returned, it is safe to re-submit the same take object via 
@ref work_queue_submit.
@param q A work queue object.
@param str A JSON description of a task. 

{ "command_line" : <i>string</i> , "output_files" : <i>array of objects with one object per output file</i> -> 
[ { "local_name" : <i>string</i> , "remote_name" : <i>string</i> , "flags" : <i>object</i> -> { 
"WORK_QUEUE_CACHE" : <i>boolean</i> , "WORK_QUEUE_NOCACHE" : <i>boolean</i> , "WORK_QUEUE_WATCH" : 
<i>boolean</i> } } ] , "input _files" : <i>array of objects with one object per input file</i> -> [ { 
"local_name" : <i>string</i> , "remote_name" : <i>string</i> , "flags" : <i>object</i> -> { "WORK_QUEUE_CACHE" : 
<i>boolean</i> , "WORK_QUEUE_NOCACHE" : <i>boolean</i> , "WORK_QUEUE_WATCH" : <i>boolean</i> } } ] , 
"tag" : <i>string</i> }

@return An integer taskid assigned to the submitted task.
*/
int work_queue_json_submit(struct work_queue *q, const char* str);

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
char* work_queue_json_wait(struct work_queue *q, int timeout);

#endif
