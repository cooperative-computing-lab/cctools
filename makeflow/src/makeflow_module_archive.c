/*
 Copyright (C) 2018- The University of Notre Dame
 This software is distributed under the GNU General Public License.
 See the file COPYING for details.
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "hash_table.h"
#include "itable.h"
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

	/* Runtime data structs */
	struct itable *task_ids;
	struct hash_table *file_ids;
	char *source_makeflow;
};

struct archive_instance *archive_instance_create()
{
	struct archive_instance *a = calloc(1,sizeof(*a));

	a->dir = NULL;

	a->task_ids = itable_create(0);
	a->file_ids = hash_table_create(0,0);

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
	itable_clear(a->task_ids);
	itable_delete(a->task_ids);
	hash_table_clear(a->file_ids);
	hash_table_delete(a->file_ids);
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

/* Return the content based ID for a file.
 * generates the checksum of a file's contents if does not exist */
static char * generate_file_archive_id(struct archive_instance *a, struct batch_file *f) {
	if(!hash_table_lookup(a->file_ids, f->outer_name)){
		unsigned char digest[SHA1_DIGEST_LENGTH];
		sha1_file(f->outer_name, digest);
		hash_table_insert(a->file_ids, f->outer_name, xxstrdup(sha1_string(digest)));
	}	
	return xxstrdup(hash_table_lookup(a->file_ids, f->outer_name));
}

/* Return the content based ID for a node.
 * This includes :
 *  command
 *  input files (content)
 *  output files (name) : 
 *	    important addition as changed expected outputs may not 
 *	    be reflected in the command and not present in archive
 *  LATER : environment variables (name:value)
 **/
static char * generate_task_archive_id(struct archive_instance *a, struct batch_task *t) {
	if(!itable_lookup(a->task_ids, t->taskid)){
		struct batch_file *f;
		unsigned char digest[SHA1_DIGEST_LENGTH];

		sha1_context_t context;
		sha1_init(&context);

		/* Add command to the archive id */
		sha1_update(&context, "C", 1);
		sha1_update(&context, t->command, strlen(t->command));
		sha1_update(&context, "\0", 1);

		/* Sort inputs for consistent hashing */
		list_sort(t->input_files, batch_file_outer_compare);

		/* add checksum of the node's input files together */
		struct list_cursor *cur = list_cursor_create(t->input_files);
		for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
			printf("input file : %s\n", f->outer_name);
			char * file_id = generate_file_archive_id(a, f);
			sha1_update(&context, "I", 1);
			sha1_update(&context, file_id, strlen(file_id));
			sha1_update(&context, "\0", 1);
			free(file_id);
		}
		list_cursor_destroy(cur);

		/* Sort outputs for consistent hashing */
		list_sort(t->output_files, batch_file_outer_compare);

		/* add checksum of the node's output file names together */
		cur = list_cursor_create(t->output_files);
		for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
			printf("output file : %s\n", f->outer_name);
			sha1_update(&context, "O", 1);
			sha1_update(&context, f->outer_name, strlen(f->outer_name));
			sha1_update(&context, "\0", 1);
		}
		list_cursor_destroy(cur);

		sha1_final(digest, &context);
		itable_insert(a->task_ids, t->taskid, xxstrdup(sha1_string(digest)));
	}
	return xxstrdup(itable_lookup(a->task_ids, t->taskid));
}

/* writes the run_info files that is stored within each archived node */
static int makeflow_archive_task_info(struct archive_instance *a, struct dag_node *n, struct batch_task *t, char *archive_path) {

	struct batch_file *f;
	char *task_info = string_format("%s/task_info", archive_path);

	FILE *fp = fopen(task_info, "w");
	if (fp == NULL) {
		free(task_info);
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create task_info for node %d archive", n->nodeid);
		return 1;
	} else {
		fprintf(fp, "COMMAND : %s\n", t->command);
		fprintf(fp, "SRC_COMMAND : %s\n", n->command);
		fprintf(fp, "SRC_MAKEFLOW : %s\n", a->source_makeflow);
		fprintf(fp, "INPUT_FILES : \n");
		struct list_cursor *cur = list_cursor_create(t->input_files);
		for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
			char *id = generate_file_archive_id(a, f);
			fprintf(fp, "\t%s : %s\n", f->outer_name, id);
			free(id);
		}
		list_cursor_destroy(cur);

		fprintf(fp, "OUTPUT_FILES : \n");
		cur = list_cursor_create(t->output_files);
		for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
			char *id = generate_file_archive_id(a, f);
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
		return 1;
	} else {
		fprintf(fp, "SUBMITTED : %" PRIu64 "\n", t->info->submitted);
		fprintf(fp, "STARTED : %" PRIu64 "\n", t->info->started);
		fprintf(fp, "FINISHED : %" PRIu64 "\n", t->info->finished);
		fprintf(fp, "EXIT_NORMAL : %d\n", t->info->exited_normally);
		fprintf(fp, "EXIT_CODE : %d\n", t->info->exit_code);
		fprintf(fp, "EXIT_SIGNAL : %d\n", t->info->exit_signal);
	}
	fclose(fp);
	free(task_info);

	return 0;
}

/* writes the file symlink that links to the archived job that created it */
static int write_file_checksum(struct archive_instance *a, struct batch_file *f, char *job_file_archive_path) {
	char * id = generate_file_archive_id(a, f);
	struct stat buf;
	int rv = 0;

	char * file_archive_dir = string_format("%s/files/%.2s", a->dir, id);
	char * file_archive_path = string_format("%s/%s", file_archive_dir, id);

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

static int makeflow_archive_write_input_files(struct archive_instance *a, struct batch_task *t, char *archive_directory_path) {
	struct batch_file *f;

	struct list_cursor *cur = list_cursor_create(t->input_files);
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		char *input_file_path = string_format("%s/input_files/%s", archive_directory_path,  f->outer_name);
		int failed_checksum = write_file_checksum(a, f, input_file_path);
		free(input_file_path);
		if(failed_checksum){
			list_cursor_destroy(cur);
			return 0;
		}
	}
	list_cursor_destroy(cur);
	return 1;
}

static int makeflow_archive_write_output_files(struct archive_instance *a, struct batch_task *t, char *archive_directory_path) {
	struct batch_file *f;

	struct list_cursor *cur = list_cursor_create(t->output_files);
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		char *output_file_path = string_format("%s/output_files/%s", archive_directory_path,  f->outer_name);
		int failed_checksum = write_file_checksum(a, f, output_file_path);
		free(output_file_path);
		if(failed_checksum){
			list_cursor_destroy(cur);
			return 0;
		}
	}
	list_cursor_destroy(cur);
	return 1;
}

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

static int makeflow_archive_populate(struct archive_instance *a, struct dag_node *n, struct batch_task *t) {
	char *id = generate_task_archive_id(a, t);
	int result = 1;

	/* The archive name is binned by the first 2 characters of the id for compactness */
	char *archive_directory_path = string_format("%s/tasks/%.2s/%s", a->dir, id, id);
	debug(D_MAKEFLOW_HOOK, "archiving task %d to %s", t->taskid, archive_directory_path);

	int dir_create = 0;
	/* We create all the sub directories upfront for convenience */
	dir_create = makeflow_archive_create_dir(archive_directory_path, "/output_files/");
	dir_create += makeflow_archive_create_dir(archive_directory_path, "/input_files/");
	dir_create += makeflow_archive_create_dir(archive_directory_path, "/descendants/");
	dir_create += makeflow_archive_create_dir(archive_directory_path, "/ancestors/");

	if(dir_create){
		result = 0;
		goto FAIL;
	}

	/* Log the task info in the task directory */
	if(makeflow_archive_task_info(a, n, t, archive_directory_path)){
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


//static int node_submit( void * instance_struct, struct dag_node *n, struct batch_task *t){
static int batch_submit( void * instance_struct, struct batch_task *t){
	struct archive_instance *a = (struct archive_instance*)instance_struct;
	int rc = MAKEFLOW_HOOK_SUCCESS;
	char *id = generate_task_archive_id(a, t);
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

	char *id = generate_task_archive_id(a, t);
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
		int archived = makeflow_archive_populate(a, n, t);
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


