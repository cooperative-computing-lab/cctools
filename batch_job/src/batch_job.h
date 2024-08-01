/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BATCH_TASK_H
#define BATCH_TASK_H

#include "list.h"
#include "jx.h"
#include "rmsummary.h"

struct batch_queue;
struct batch_file;

struct batch_job {
	int taskid;                  /* Indicates the id provided by the creating system. I.E. Makeflow */
	int jobid;                   /* Indicates the id assigned to the job by the submission system. */

	struct batch_queue *queue;   /* The queue this task is assigned to. */

	char *command;               /* The command line to execute. */

	struct list   *input_files;  /* Task's required inputs, type batch_file */
	struct list   *output_files; /* Task's expected outputs, type batch_file */

	struct rmsummary *resources; /* Resources assigned to task */

	struct jx *envlist;          /* JSON formatted environment list */ 

	struct batch_job_info *info; /* Stores the info struct created by batch_queue. */

	char *hash;                  /* Checksum based on CMD, input contents, and output names. */
};

/** Create a batch_job struct.
@param queue The queue this task is associated with/assigned to.
@return A batch_job struct in newly alloced space.
*/
struct batch_job *batch_job_create(struct batch_queue *queue );

/** Delete a batch_job struct.
 This frees the command, deletes the files and lists, deletes the resources, and deletes envlist.
@param t The batch_job struct to be freed.
*/
void batch_job_delete(struct batch_job *t);

/** Add file to input list of batch_job
 Creates a new batch_file from outer_name and inner_name.
 This newly created file is add to input_files.
 For clarifications on outer_name, inner_name, and their uses see batch_file.
@param task The batch_job this file is being added to.
@param outer_name The name of the file at submission/host site.
@param inner_name The name of the file at execution site.
@return A pointer to the newly allocated batch_file struct.
*/
struct batch_file * batch_job_add_input_file(struct batch_job *task, const char * outer_name, const char * inner_name);

/** Add file to output list of batch_job
 Creates a new batch_file from outer_name and inner_name.
 This newly created file is add to output_files.
 For clarifications on outer_name, inner_name, and their uses see batch_file.
@param task The batch_job this file is being added to.
@param outer_name The name of the file at submission/host site.
@param inner_name The name of the file at execution site.
@return A pointer to the newly allocated batch_file struct.
*/
struct batch_file * batch_job_add_output_file(struct batch_job *task, const char * outer_name, const char * inner_name);

/** Set the command of the batch_job.
 Frees previous command and xxstrdups new command.
@param t The batch_job to be updated.
@param command The new command to use.
*/
void batch_job_set_command(struct batch_job *t, const char *command);

/** Set the batch task's command to the given JX command spec.
 * The JX command spec is first expanded, and replaces the
 * batch task's previous command.
 * @param t The batch_job to be updated.
 * @param command The spec to use.
 */
void batch_job_set_command_spec(struct batch_job *t, struct jx *command);

/** Wrap the existing command with a template string.
 This uses string_wrap_command to wrap command, see stringtools.h for details.
 This function allocates a new string with the result and free the previous command.
 Does not free passed command. Will use wrapper interface in future.
@param t The batch_job whose command is being wrapped.
@param command The command template that will wrap existing command.
*/
void batch_job_wrap_command(struct batch_job *t, const char *command);

/** Set the resources needed for task.
 This function will make a copy of full copy of resources using rmsummary_copy().
@param t The batch_job requiring specified resources.
@param resources A rmsummary specifying required resources.
*/
void batch_job_set_resources(struct batch_job *t, const struct rmsummary *resources);

/** Set the envlist for this task.
 This function will make a copy using jx_copy of envlist.
@param t The batch_job using this environment.
@param envlist The jx_object specifying the environment.
*/
void batch_job_set_envlist(struct batch_job *t, struct jx *envlist);

/** Set the batch_job_info of this task.
 Performs simple copy into already allocated memory.
@param t The batch_job that was completed.
@param info The batch_job_info of the completed task.
*/
void batch_job_set_info(struct batch_job *t, struct batch_job_info *info);

/** Generate a sha1 hash based on the specified task.
 Includes Command, Input files contents, Output files names
 Future improvement should include the Environment
@param t The batch_job whose checksum will be generated.
@return Allocated string of the hash, user should free.
*/
char * batch_job_generate_id(struct batch_job *t);

#endif
/* vim: set noexpandtab tabstop=8: */
