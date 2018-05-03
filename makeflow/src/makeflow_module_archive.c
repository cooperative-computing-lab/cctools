/*
 Copyright (C) 2016- The University of Notre Dame
 This software is distributed under the GNU General Public License.
 See the file COPYING for details.

Authorship : Pierce Cunneen
Update/Migrated to hook: Nick Hazekamp
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "list.h"
#include "set.h"
#include "sha1.h"
#include "stringtools.h"
#include "timestamp.h"
#include "xxmalloc.h"

#include "batch_job.h"
#include "batch_wrapper.h"

#include "dag.h"
#include "dag_file.h"
#include "dag_node.h"
#include "makeflow_gc.h"
#include "makeflow_log.h"
#include "makeflow_hook.h"

#define MAKEFLOW_ARCHIVE_DEFAULT_DIRECTORY "/tmp/makeflow.archive."

struct archive_instance {
	/* User defined values */
	int read;
	int write;
	int found_archived_job;
	char *dir;

	/* Runtime data struct */
	char *source_makeflow;
};

struct archive_instance *archive_instance_create()
{
	struct archive_instance *a = calloc(1,sizeof(*a));

	a->dir = NULL;
	a->source_makeflow = NULL;

	return a;
}

static int create( void ** instance_struct, struct jx *hook_args )
{
	struct archive_instance *a = archive_instance_create();
	*instance_struct = a;

	if(jx_lookup_string(hook_args, "archive_dir")){
		a->dir = xxstrdup(jx_lookup_string(hook_args, "archive_dir"));	
	} else {
		a->dir = string_format("%s%d", MAKEFLOW_ARCHIVE_DEFAULT_DIRECTORY, getuid());
	}

	if(jx_lookup_integer(hook_args, "archive_read")){
		a->read = 1;
	}

	if(jx_lookup_integer(hook_args, "archive_write")){
		a->write = 1;
	}

	return MAKEFLOW_HOOK_SUCCESS;
}

static int destroy( void * instance_struct, struct dag *d)
{
	struct archive_instance *a = (struct archive_instance*)instance_struct;

	free(a->dir);
	free(a->source_makeflow);
	free(a);
	return MAKEFLOW_HOOK_SUCCESS;
}

static int dag_check( void * instance_struct, struct dag *d){
	struct archive_instance *a = (struct archive_instance*)instance_struct;

	unsigned char digest[SHA1_DIGEST_LENGTH];
	sha1_file(d->filename, digest);
	a->source_makeflow = xxstrdup(sha1_string(digest));
	
	if (a->write) {
		char *source_makeflow_file_path = string_format("%s/makeflows/%.2s/%s", a->dir, a->source_makeflow, a->source_makeflow);
		int success = copy_file_to_file(d->filename, source_makeflow_file_path);
		free(source_makeflow_file_path);
		if (!success) {
			debug(D_ERROR|D_MAKEFLOW_HOOK, "Could not archive source makeflow file %s\n", source_makeflow_file_path);
			return MAKEFLOW_HOOK_FAILURE;
		} else {
			debug(D_MAKEFLOW_HOOK, "Source makeflow %s stored at %s\n", d->filename, source_makeflow_file_path);
		}
	}
	return MAKEFLOW_HOOK_SUCCESS;
}

static int dag_loop( void * instance_struct, struct dag *d){
	struct archive_instance *a = (struct archive_instance*)instance_struct;
	/*Note:
	Due to the fact that archived tasks are never "run", no local or remote tasks are added
	to the remote or local job table if all ready tasks were found within the archive.
	Thus makeflow_dispatch_ready_tasks must run at least once more if an archived job was found.
	*/
	if(a->found_archived_job == 1){
		a->found_archived_job = 0;
		return MAKEFLOW_HOOK_SUCCESS;
	}
	return MAKEFLOW_HOOK_END;
}

/* Write the task and run info to the task directory
 *	These files are hardcoded to task_info and run_info
 *
 * task_info :
 *	COMMAND: Tasks command that was run
 *	SRC_COMMAND: Origin node's command for reference
 *	SRC_LINE:  Line of origin node in SRC_MAKEFLOW
 *	SRC_MAKEFLOW:  ID of file for the original Makeflow stored in archive
 *	INPUT_FILES: Alphabetic list of input files checksum IDs
 *	OUTPUT_FILES: Alphabetic list of output file outer_names
 *
 * run_info : 
 *  SUBMITTED : Time task was submitted
 *  STARTED : Time task was started
 *  FINISHED : Time task was completed
 *  EXIT_NORMALLY : 0 if abnormal exit, 1 is normal
 *  EXIT_CODE : Task's exit code
 *  EXIT_SIGNAL : Int value of signal if occurred
 */
static int makeflow_archive_write_task_info(struct archive_instance *a, struct dag_node *n, struct batch_task *t, char *archive_path) {

	struct batch_file *f;
	char *task_info = string_format("%s/task_info", archive_path);

	FILE *fp = fopen(task_info, "w");
	if (fp == NULL) {
		free(task_info);
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create task_info for node %d archive", n->nodeid);
		return 0;
	} else {
		fprintf(fp, "COMMAND : %s\n", t->command);
		fprintf(fp, "SRC_COMMAND : %s\n", n->command);
		fprintf(fp, "SRC_LINE : %d\n", n->linenum);
		fprintf(fp, "SRC_MAKEFLOW : %s\n", a->source_makeflow);
		fprintf(fp, "INPUT_FILES : \n");
		struct list_cursor *cur = list_cursor_create(t->input_files);
		for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
			char *id = batch_file_generate_id(f);
			fprintf(fp, "\t%s : %s\n", f->outer_name, id);
			free(id);
		}
		list_cursor_destroy(cur);

		fprintf(fp, "OUTPUT_FILES : \n");
		cur = list_cursor_create(t->output_files);
		for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
			char *id = batch_file_generate_id(f);
			fprintf(fp, "\t%s : %s\n", f->outer_name, id);
			free(id);
		}
		list_cursor_destroy(cur);
	}
	fclose(fp);
	free(task_info);

	task_info = string_format("%s/run_info", archive_path);

	fp = fopen(task_info, "w");
	if (fp == NULL) {
		free(task_info);
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create run_info for node %d archive", n->nodeid);
		return 0;
	} else {
		fprintf(fp, "SUBMITTED : %lu\n", t->info->submitted);
		fprintf(fp, "STARTED : %lu\n", t->info->started);
		fprintf(fp, "FINISHED : %lu\n", t->info->finished);
		fprintf(fp, "EXIT_NORMAL : %d\n", t->info->exited_normally);
		fprintf(fp, "EXIT_CODE : %d\n", t->info->exit_code);
		fprintf(fp, "EXIT_SIGNAL : %d\n", t->info->exit_signal);
	}
	fclose(fp);
	free(task_info);

	return 1;
}

/* Archive the specified file.
 * This includes several steps:
 *	1. Generate the id
 *	2. Copy file to id if non-existent
 *	3. Link back to creating task
 *
@return 0 if successfully archived, 1 if failed at any point.
 */
static int makeflow_archive_file(struct archive_instance *a, struct batch_file *f, char *job_file_archive_path) {
	/* Generate the file archive id (content based) if does not exist. */
	char * id = batch_file_generate_id(f);
	struct stat buf;
	int rv = 0;

	char * file_archive_dir = string_format("%s/files/%.2s", a->dir, id);
	char * file_archive_path = string_format("%s/%s", file_archive_dir, id);

	/* Create the archive path with 2 character prefix. */
	if (!create_dir(file_archive_dir, 0777) && errno != EEXIST){
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create file archiving directory %s\n", file_archive_dir);
		rv = 1;
		goto FAIL;
	}

	/* Check if file is already archived */
	if(stat(file_archive_path, &buf) >= 0) {
		debug(D_MAKEFLOW_HOOK, "file %s already archived at %s", f->outer_name, file_archive_path);
	/* File did not already exist, store in general file area */
	} else if (!copy_file_to_file(f->outer_name, file_archive_path)){
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not archive output file %s at %s\n",f->outer_name, file_archive_path);
		rv = 1;
		goto FAIL;
	}

	/* Create a symlink to task that used/created this file. */
	int symlink_failure = symlink(file_archive_path, job_file_archive_path);
	if (symlink_failure && errno != EEXIST) {
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create symlink %s pointing to %s\n", job_file_archive_path, file_archive_path);
		rv = 1;
		goto FAIL;
	}

FAIL:
	free(id);
	free(file_archive_dir);
	free(file_archive_path);
	return rv;
}

/* Loop over inputs and archive each file */
static int makeflow_archive_write_input_files(struct archive_instance *a, struct batch_task *t, char *archive_directory_path) {
	struct batch_file *f;

	struct list_cursor *cur = list_cursor_create(t->input_files);
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		char *input_file_path = string_format("%s/input_files/%s", archive_directory_path,  f->outer_name);
		int failed_checksum = makeflow_archive_file(a, f, input_file_path);
		free(input_file_path);
		if(failed_checksum){
			list_cursor_destroy(cur);
			return 0;
		}
	}
	list_cursor_destroy(cur);
	return 1;
}

/* Loop over outputs and archive each file */
static int makeflow_archive_write_output_files(struct archive_instance *a, struct batch_task *t, char *archive_directory_path) {
	struct batch_file *f;

	struct list_cursor *cur = list_cursor_create(t->output_files);
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		char *output_file_path = string_format("%s/output_files/%s", archive_directory_path,  f->outer_name);
		int failed_checksum = makeflow_archive_file(a, f, output_file_path);
		free(output_file_path);
		if(failed_checksum){
			list_cursor_destroy(cur);
			return 0;
		}
	}
	list_cursor_destroy(cur);
	return 1;
}

/* Using the task prefix, creates the specified directory and checks for failure. */
static int makeflow_archive_create_dir(char * prefix, char * name){
	char *tmp_directory_path = string_format("%s%s", prefix, name);
	int created = create_dir(tmp_directory_path, 0777);
	free(tmp_directory_path);
	if (!created){
		debug(D_ERROR|D_MAKEFLOW_HOOK,"Could not create archiving directory %s\n", tmp_directory_path);
		return 1;
	}
	return 0;
}

/* Archive a batch_task.
 * Archiving requires several steps:
 *  1. Create task directory structure
 *  2. Write out task information
 *  3. Archive inputs
 *  4. Archive outputs
 *
@return 1 if archive was successful, 0 if archive failed.
 */
static int makeflow_archive_task(struct archive_instance *a, struct dag_node *n, struct batch_task *t) {
	/* Generate the task id */
	char *id = batch_task_generate_id(t);
	int result = 1;

	/* The archive name is binned by the first 2 characters of the id for compactness */
	char *archive_directory_path = string_format("%s/tasks/%.2s/%s", a->dir, id, id);
	debug(D_MAKEFLOW_HOOK, "archiving task %d to %s", t->taskid, archive_directory_path);

	int dir_create_error = 0;
	/* We create all the sub directories upfront for convenience */
	dir_create_error = makeflow_archive_create_dir(archive_directory_path, "/output_files/");
	dir_create_error += makeflow_archive_create_dir(archive_directory_path, "/input_files/");
	dir_create_error += makeflow_archive_create_dir(archive_directory_path, "/descendants/");
	dir_create_error += makeflow_archive_create_dir(archive_directory_path, "/ancestors/");

	if(dir_create_error){
		result = 0;
		goto FAIL;
	}

	/* Log the task info in the task directory */
	if(!makeflow_archive_write_task_info(a, n, t, archive_directory_path)){
		result = 0;
		goto FAIL;
	}


	if(!makeflow_archive_write_input_files(a, t, archive_directory_path)){
		result = 0;
		goto FAIL;
	}
	if(!makeflow_archive_write_output_files(a, t, archive_directory_path)){
		result = 0;
		goto FAIL;
	}

	debug(D_MAKEFLOW_HOOK, "node %d successfully archived", n->nodeid);

FAIL:
	free(archive_directory_path);
	free(id);
	return result;
}

int makeflow_archive_copy_preserved_files(struct archive_instance *a, struct batch_task *t, char *task_path ) {
	struct batch_file *f;

	struct list_cursor *cur = list_cursor_create(t->output_files);
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		char *output_file_path = string_format("%s/output_files/%s",task_path, f->outer_name);
		int success = copy_file_to_file(output_file_path, f->outer_name);
		free(output_file_path);
		if (!success) {
			list_cursor_destroy(cur);
			debug(D_ERROR|D_MAKEFLOW_HOOK,"Failed to copy output file %s to %s\n", output_file_path, f->outer_name);
			return 1;
		}
	}
	list_cursor_destroy(cur);

	return 0;
}

int makeflow_archive_is_preserved(struct archive_instance *a, struct batch_task *t, char *task_path) {
	struct batch_file *f;
	struct stat buf;

	if((stat(task_path, &buf) < 0)){
		/* Not helpful unless you know the task number. */
		debug(D_MAKEFLOW_HOOK, "task %d has not been previously archived at %s", t->taskid, task_path);
		return 0;
	}

	struct list_cursor *cur = list_cursor_create(t->output_files);
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		char *filename = string_format("%s/output_files/%s", task_path, f->outer_name);
		int file_exists = stat(filename, &buf);
		free(filename);
		if (file_exists < 0) {
			list_cursor_destroy(cur);
			debug(D_MAKEFLOW_HOOK, "output file %s not found in archive at %s", f->outer_name, task_path);
			return 0;
		}
	}
	list_cursor_destroy(cur);

	return 1;
}

static int batch_submit( void * instance_struct, struct batch_task *t){
	struct archive_instance *a = (struct archive_instance*)instance_struct;
	int rc = MAKEFLOW_HOOK_SUCCESS;
	char *id = batch_task_generate_id(t);
	char *task_path = string_format("%s/tasks/%.2s/%s",a->dir, id, id);
	debug(D_MAKEFLOW_HOOK, "Checking archive for task %d at %.5s\n", t->taskid, id);

	if(a->read && makeflow_archive_is_preserved(a, t, task_path)){
		debug(D_MAKEFLOW_HOOK, "Task %d already exists in archive, replicating output files\n", t->taskid);

		/* copy archived files to working directory and update state for node and dag_files */
		makeflow_archive_copy_preserved_files(a, t, task_path);
		t->info->exited_normally = 1;
		a->found_archived_job = 1;
		printf("job %d was pulled from archive\n", t->taskid);
		rc = MAKEFLOW_HOOK_SKIP;
	}

	free(id);
	free(task_path);
	return rc;
}

static int batch_retrieve( void * instance_struct, struct batch_task *t){
	struct archive_instance *a = (struct archive_instance*)instance_struct;
	int rc = MAKEFLOW_HOOK_SUCCESS;

	char *id = batch_task_generate_id(t);
	char *task_path = string_format("%s/tasks/%.2s/%s",a->dir, id, id);
	if(a->read && makeflow_archive_is_preserved(a, t, task_path)){
		debug(D_MAKEFLOW_HOOK, "Task %d run was bypassed using archive\n", t->taskid);
		rc = MAKEFLOW_HOOK_RUN;
	}

	free(id);
	free(task_path);
	return rc;
}

static int node_success( void * instance_struct, struct dag_node *n, struct batch_task *t){
	struct archive_instance *a = (struct archive_instance*)instance_struct;
	/* store node into archiving directory  */
	if (a->write) {
		debug(D_MAKEFLOW_HOOK, "archiving task %d in directory: %s\n",t->taskid, a->dir);
		int archived = makeflow_archive_task(a, n, t);
		if(!archived){
			debug(D_MAKEFLOW_HOOK, "unable to archive task %d in directory: %s\n",t->taskid, a->dir);
			return MAKEFLOW_HOOK_FAILURE;
		}
	}

	return MAKEFLOW_HOOK_SUCCESS;
}

struct makeflow_hook makeflow_hook_archive = {
	.module_name = "Archive",
	.create = create,
	.destroy = destroy,

	.dag_check = dag_check,
	.dag_loop = dag_loop,

	.batch_submit = batch_submit,
	.batch_retrieve = batch_retrieve,

	.node_success = node_success,
};


