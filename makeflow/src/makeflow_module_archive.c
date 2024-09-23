/*
 Copyright (C) 2022 The University of Notre Dame
 This software is distributed under the GNU General Public License.
 See the file COPYING for details.

Authorship : Pierce Cunneen
Update/Migrated to hook: Nick Hazekamp
Updated for S3 bucket handling and directory handling for input and output files: Nicholas Potteiger
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <libgen.h>
#include <time.h>
#include <sys/time.h>

#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "list.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_pretty_print.h"
#include "path.h"
#include "set.h"
#include "sha1.h"
#include "stringtools.h"
#include "timestamp.h"
#include "unlink_recursive.h"
#include "xxmalloc.h"
#include "hash_table.h"
#include "copy_tree.h"
#include "s3_file_io.h"

#include "batch_queue.h"
#include "batch_wrapper.h"

#include "dag.h"
#include "dag_file.h"
#include "dag_node.h"
#include "makeflow_gc.h"
#include "makeflow_log.h"
#include "makeflow_hook.h"

#define MAKEFLOW_ARCHIVE_DEFAULT_DIRECTORY "/tmp/makeflow.archive."
#define MAKEFLOW_ARCHIVE_DEFAULT_S3_BUCKET "makeflows3archive"

float total_up_time = 0.0;
float total_down_time = 0.0;
float total_s3_check_time = 0.0;
struct hash_table *s3_files_in_archive = NULL;

struct archive_instance {
	/* User defined values */
	int read;
	int write;
	int found_archived_job;
	int s3;
	int s3_check;
	char *dir;
	char *s3_dir;

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
	aws_init ();
        aws_set_debug (0);
        char line[256];

	if(s3_files_in_archive == NULL){
				s3_files_in_archive = hash_table_create(0,0);
		}

	struct archive_instance *a = archive_instance_create();
	*instance_struct = a;

	if(jx_lookup_string(hook_args, "archive_dir")){
		a->dir = xxstrdup(jx_lookup_string(hook_args, "archive_dir"));	
	} else {
		a->dir = string_format("%s%d", MAKEFLOW_ARCHIVE_DEFAULT_DIRECTORY, getuid());
	}

	if(jx_lookup_boolean(hook_args, "archive_s3_no_check")){
		a->s3_check = 0;
	}
	else{
		a->s3_check = 1;
	}

	if(jx_lookup_string(hook_args, "archive_s3_arg")){
		a->s3 = 1;
		a->s3_dir = xxstrdup(jx_lookup_string(hook_args, "archive_s3_arg"));
	}
	else if(jx_lookup_string(hook_args, "archive_s3_no_arg")) {
		a->s3 = 1;
		a->s3_dir = string_format("%s",MAKEFLOW_ARCHIVE_DEFAULT_S3_BUCKET);
	}
	else{
		a->s3 = 0;
	}

	if(jx_lookup_string(hook_args, "s3_hostname")){
		s3_set_host(xxstrdup(jx_lookup_string(hook_args, "s3_hostname")));
	}

	if(jx_lookup_string(hook_args, "s3_keyid")){
		aws_set_keyid(xxstrdup(jx_lookup_string(hook_args, "s3_keyid")));	
	}
	else{
		FILE *fp = popen("grep aws_access_key_id ~/.aws/credentials | cut -d ""="" -f 2 | tr -d ' '","r");
        	fgets(line, 255, fp);
        	line[strlen(line)-1] = '\0';
        	aws_set_keyid(line);
        	pclose(fp);	
	}

	if(jx_lookup_string(hook_args, "s3_secretkey")){
		aws_set_key(xxstrdup(jx_lookup_string(hook_args, "s3_secretkey")));
	}
	else{
		FILE *ft = popen("grep aws_secret_access_key ~/.aws/credentials | cut -d ""="" -f 2 | tr -d ' '","r");
        	fgets(line, 255, ft);
        	line[strlen(line)-1] = '\0';
        	aws_set_key(line);
        	pclose(ft);
	}

	if(jx_lookup_boolean(hook_args, "archive_read")){
		a->read = 1;
	}

	if(jx_lookup_boolean(hook_args, "archive_write")){
		a->write = 1;
	}

	if (!create_dir(a->dir, 0777) && errno != EEXIST){
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create base archiving directory %s: %d %s\n", 
			a->dir, errno, strerror(errno));
		return MAKEFLOW_HOOK_FAILURE;
	}

	char *files_dir = string_format("%s/files", a->dir);
	if (!create_dir(files_dir, 0777) && errno != EEXIST){
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create files archiving directory %s: %d %s\n", 
			files_dir, errno, strerror(errno));
		free(files_dir);
		return MAKEFLOW_HOOK_FAILURE;
	}
	free(files_dir);

	char *tasks_dir = string_format("%s/tasks", a->dir);
	if (!create_dir(tasks_dir, 0777) && errno != EEXIST){
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create tasks archiving directory %s: %d %s\n", 
			tasks_dir, errno, strerror(errno));
		free(tasks_dir);
		return MAKEFLOW_HOOK_FAILURE;
	}
	free(tasks_dir);

	s3_set_bucket (a->s3_dir);

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
	// Takes hash of filename and stores it in digest
	sha1_file(d->filename, digest);
	// Makes a c string copy of your hash address
	a->source_makeflow = xxstrdup(sha1_string(digest));
	// If a is in write mode using the -w flag
	if (a->write) {
		// Formats archive file directory
		char *source_makeflow_file_dir = string_format("%s/files/%.2s", a->dir, a->source_makeflow);
		// If the directory was not able to be created and directory already exists
		if (!create_dir(source_makeflow_file_dir, 0777) && errno != EEXIST){
			// Prints out a debug error
			debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create makeflow archiving directory %s: %d %s\n", 
				source_makeflow_file_dir, errno, strerror(errno));
			// Frees memory for source_makeflow_file_dir
			free(source_makeflow_file_dir);
			return MAKEFLOW_HOOK_FAILURE;
		}
		// Gets the path of the archive file
		char *source_makeflow_file_path = string_format("%s/%s", source_makeflow_file_dir, a->source_makeflow);
		// Frees memory
		free(source_makeflow_file_dir);
		// If the file was unable to be copied to the new path
		if (!copy_file_to_file(d->filename, source_makeflow_file_path)){
			// Prints out debug error
			debug(D_ERROR|D_MAKEFLOW_HOOK, "Could not archive source makeflow file %s\n", source_makeflow_file_path);
			free(source_makeflow_file_path);
			return MAKEFLOW_HOOK_FAILURE;
		} else {
			// Print out where it is stored at
			debug(D_MAKEFLOW_HOOK, "Source makeflow %s stored at %s\n", d->filename, source_makeflow_file_path);
		}
		free(source_makeflow_file_path);
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

static int makeflow_archive_task_adheres_to_sandbox( struct batch_job *t ){
	int rc = 0;
	struct batch_file *f;
	struct list_cursor *cur = list_cursor_create(t->input_files);
	// Iterate through input files
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		// If your using an absolute path or trying to get our of a directory using the (..)
		if(path_has_doubledots(f->inner_name) || f->inner_name[0] == '/'){
			// Print out a debug error
			debug(D_MAKEFLOW_HOOK, 
				"task %d will not be archived as input file %s->%s does not adhere to the sandbox model of execution", 
				t->taskid, f->outer_name, f->inner_name);
			rc = 1;
		}
	}
	// Frees memory of list cursor
	list_cursor_destroy(cur);
	

	cur = list_cursor_create(t->output_files);
	// Iterates through output files
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		// Same check as input files
		if(path_has_doubledots(f->inner_name) || f->inner_name[0] == '/'){
			// Prints out a debug error
			debug(D_MAKEFLOW_HOOK, 
				"task %d will not be archived as output file %s->%s does not adhere to the sandbox model of execution", 
				t->taskid, f->outer_name, f->inner_name);
			rc = 1;
		}
	}
	// Frees memory of list cursor
	list_cursor_destroy(cur);

	return rc;
}

/* Overall structure of an archive unit:
 * archive_dir --> tasks --> checksum_pre(2 digits) --> checksum --> task_info
 *            |                                                 |--> run_info
 *            |                                                 |--> input_files --> file_name(symlink to actual file)
 *            |                                                 |--> output_files --> file_name(symlink to actual file)
 *            |--> files --> checksum_pre(2 digits) --> checksum (actual file)
 */

/* Write the task and run info to the task directory
 *	These files are hardcoded to task_info and run_info */
static int makeflow_archive_write_task_info(struct archive_instance *a, struct dag_node *n, struct batch_job *t, char *archive_path) {
	struct batch_file *f;

/* task_info :
 *	COMMAND: Tasks command that was run
 *	SRC_COMMAND: Origin node's command for reference
 *	SRC_LINE:  Line of origin node in SRC_MAKEFLOW
 *	SRC_MAKEFLOW:  ID of file for the original Makeflow stored in archive
 *	INPUT_FILES: Alphabetic list of input files checksum IDs
 *	OUTPUT_FILES: Alphabetic list of output file inner_names
 */
	struct jx *task_jx = jx_object(NULL);
	jx_insert(task_jx, jx_string("COMMAND"), jx_string(t->command));
	jx_insert(task_jx, jx_string("SRC_COMMAND"), jx_string(n->command));
	jx_insert(task_jx, jx_string("SRC_LINE"), jx_integer(n->linenum));
	jx_insert(task_jx, jx_string("SRC_MAKEFLOW"), jx_string(a->source_makeflow));
	struct jx * input_files = jx_object(NULL);
	struct list_cursor *cur = list_cursor_create(t->input_files);
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		/* Generate the file archive id (content based) if does not exist. */
		char * id;
		if(path_is_dir(f->inner_name) == 1){
			f->hash = batch_file_generate_id_dir(f->inner_name);
			id = xxstrdup(f->hash);
		}
		else{
			id = batch_file_generate_id(f);
		}
		jx_insert(input_files, jx_string(f->inner_name), jx_string(id));
		free(id);
	}
	list_cursor_destroy(cur);
	jx_insert(task_jx, jx_string("INPUT_FILES"), input_files);

	struct jx * output_files = jx_object(NULL);
	cur = list_cursor_create(t->output_files);
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		/* Generate the file archive id (content based) if does not exist. */
		char * id;
		if(path_is_dir(f->inner_name) == 1){
			f->hash = batch_file_generate_id_dir(f->inner_name);
			id = xxstrdup(f->hash);
		}
		else{
			id = batch_file_generate_id(f);
		}
		jx_insert(output_files, jx_string(f->inner_name), jx_string(id));
		free(id);
	}
	list_cursor_destroy(cur);
	jx_insert(task_jx, jx_string("OUTPUT_FILES"), output_files);

	char *task_info = string_format("%s/task_info", archive_path);
	FILE *fp = fopen(task_info, "w");
	if (fp == NULL) {
		free(task_info);
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create task_info for node %d archive", n->nodeid);
		return 0;
	} else {
		jx_pretty_print_stream(task_jx, fp);
	}
	fclose(fp);
	free(task_info);
	jx_delete(task_jx);

/* run_info : 
 *  SUBMITTED : Time task was submitted
 *  STARTED : Time task was started
 *  FINISHED : Time task was completed
 *  EXIT_NORMALLY : 0 if abnormal exit, 1 is normal
 *  EXIT_CODE : Task's exit code
 *  EXIT_SIGNAL : Int value of signal if occurred
 */
	struct jx * run_jx = jx_object(NULL);
	jx_insert(run_jx, jx_string("SUBMITTED"), jx_integer(t->info->submitted));
	jx_insert(run_jx, jx_string("STARTED"), jx_integer(t->info->started));
	jx_insert(run_jx, jx_string("FINISHED"), jx_integer(t->info->finished));
	jx_insert(run_jx, jx_string("EXIT_NORMAL"), jx_integer(t->info->exited_normally));
	jx_insert(run_jx, jx_string("EXIT_CODE"), jx_integer(t->info->exit_code));
	jx_insert(run_jx, jx_string("EXIT_SIGNAL"), jx_integer(t->info->exit_signal));

	task_info = string_format("%s/run_info", archive_path);

	fp = fopen(task_info, "w");
	if (fp == NULL) {
		free(task_info);
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create run_info for node %d archive", n->nodeid);
		return 0;
	} else {
		jx_pretty_print_stream(run_jx, fp);
	}
	fclose(fp);
	free(task_info);
	jx_delete(run_jx);

	return 1;
}




/* Check to see if a file is already in the s3 bucket */
static int in_s3_archive(struct archive_instance *a, char *file_name){
	char *check_sum_value = hash_table_lookup(s3_files_in_archive, file_name);
	// Check to see if file is already in the hash table before checking s3
		if(check_sum_value == NULL){
		struct timeval start_time;
		struct timeval end_time;
		gettimeofday(&start_time, NULL);
		if(s3_check(file_name) == 0){
			debug(D_MAKEFLOW_HOOK, "file/task %s does not exist in the S3 bucket: %s", file_name, a->s3_dir);
			gettimeofday(&end_time,NULL);
			float run_time = ((end_time.tv_sec*1000000 + end_time.tv_usec) - (start_time.tv_sec*1000000 + start_time.tv_usec)) / 1000000.0;
			total_s3_check_time += run_time;
			debug(D_MAKEFLOW_HOOK," It took %f seconds to check if %s is in %s",run_time, file_name, a->s3_dir);
			debug(D_MAKEFLOW_HOOK," The total s3 check time is %f second(s)",total_s3_check_time);
			return 0;
		}
		debug(D_MAKEFLOW_HOOK, "file/task %s already exists in the S3 bucket: %s", file_name, a->s3_dir);
		gettimeofday(&end_time,NULL);
		float run_time = ((end_time.tv_sec*1000000 + end_time.tv_usec) - (start_time.tv_sec*1000000 + start_time.tv_usec)) / 1000000.0;
		total_s3_check_time += run_time;
		debug(D_MAKEFLOW_HOOK," It took %f seconds to check if %s is in %s",run_time, file_name, a->s3_dir);
		debug(D_MAKEFLOW_HOOK," The total s3 check time is %f second(s)",total_s3_check_time);
		return 1;
	}
	debug(D_MAKEFLOW_HOOK, "file/task %s already cached and exists in the S3 bucket: %s",file_name, a->s3_dir);
	return 1;
}

/* Copy a file to the s3 bucket*/
static int makeflow_archive_s3_file(struct archive_instance *a, char *batchID, char *file_path){
	// Copy to s3 archive
	struct timeval start_time;
	struct timeval end_time;
	char *fileCopy;
	FILE *fp;
	//Tar directories before submitting them to s3 bucket
	if(path_is_dir(file_path) != 1){
		fp = fopen(file_path,"rb");
	}
	else{
		char *tarDir = string_format("tar -czvf %s.tar.gz -C %s .",file_path,file_path);
		if(system(tarDir) != 0){
			free(tarDir);
			return 0;
		}
		free(tarDir);
		fileCopy = string_format("%s.tar.gz",file_path);
		fp = fopen(fileCopy,"rb");
		free(fileCopy);
	}
	gettimeofday(&start_time, NULL);
	if(s3_put(fp,batchID) != 0){
		gettimeofday(&end_time,NULL);
		float run_time = ((end_time.tv_sec*1000000 + end_time.tv_usec) - (start_time.tv_sec*1000000 + start_time.tv_usec)) / 1000000.0;
		total_up_time += run_time;
		debug(D_MAKEFLOW_HOOK," It took %f seconds for %s to fail uploading to %s",run_time, batchID, a->s3_dir);
		debug(D_MAKEFLOW_HOOK," The total upload time is %f second(s)",total_up_time);
		return 0;
	}
	gettimeofday(&end_time,NULL);
		float run_time = ((end_time.tv_sec*1000000 + end_time.tv_usec) - (start_time.tv_sec*1000000 + start_time.tv_usec)) / 1000000.0;
	total_up_time += run_time;
	hash_table_insert(s3_files_in_archive, batchID, batchID);
	fclose(fp);
	printf("Upload %s to %s/%s\n",file_path, a->s3_dir, batchID);
	debug(D_MAKEFLOW_HOOK," It took %f second(s) for %s to upload to %s\n",run_time, batchID, a->s3_dir);
	debug(D_MAKEFLOW_HOOK," The total upload time is %f second(s)",total_up_time);

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
	char * id;
	if(path_is_dir(f->inner_name) == 1){
		f->hash = batch_file_generate_id_dir(f->inner_name);
				id = xxstrdup(f->hash);
	}
	else{
		id = batch_file_generate_id(f);
	}

	struct stat buf;
	int rv = 0;

	char * file_archive_dir = string_format("%s/files/%.2s", a->dir, id);
	char * file_archive_path = string_format("%s/%s", file_archive_dir, id);
	char * job_file_archive_dir = NULL;

	/* Create the archive path with 2 character prefix. */
	if (!create_dir(file_archive_dir, 0777) && errno != EEXIST){
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create file archiving directory %s: %d %s\n", 
			file_archive_dir, errno, strerror(errno));
		rv = 1;
		goto FAIL;
	}

	/* Check if file is already archived */
	if(stat(file_archive_path, &buf) >= 0) {
		debug(D_MAKEFLOW_HOOK, "file %s already archived at %s", f->outer_name, file_archive_path);
	/* File did not already exist, store in general file area */
	} else {
		if(path_is_dir(f->outer_name) != 1){
			if (!copy_file_to_file(f->outer_name, file_archive_path)){
				debug(D_ERROR|D_MAKEFLOW_HOOK, "could not archive output file %s at %s: %d %s\n",
					f->outer_name, file_archive_path, errno, strerror(errno));
				rv = 1;
				goto FAIL;
			}
		}
		else{
			debug(D_MAKEFLOW,"COPYING %s to the archive",f->outer_name);
			if(copy_dir(f->outer_name,file_archive_path) != 0){
				debug(D_ERROR|D_MAKEFLOW_HOOK, "could not archive output file %s at %s: %d %s\n",
										f->outer_name, file_archive_path, errno, strerror(errno));
								rv = 1;
								goto FAIL;
			}
		}
	}

	/* Create the directory structure for job_file_archive. */
	job_file_archive_dir = xxstrdup(job_file_archive_path);
	path_dirname(job_file_archive_path, job_file_archive_dir);
	if (!create_dir(job_file_archive_dir, 0777) && errno != EEXIST){
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create job file directory %s: %d %s\n", 
			file_archive_dir, errno, strerror(errno));
		rv = 1;
		goto FAIL;
	}

	if(a->s3){
		int result = 1;
		// Check to see if file already exists in the s3 bucket
		if(a->s3_check){
			if(!in_s3_archive(a,id)){
				result = makeflow_archive_s3_file(a,id,file_archive_path);
			}
		}
		else
			result = makeflow_archive_s3_file(a,id,file_archive_path);
		/* Copy file to the s3 bucket*/
		if(!result){
			debug(D_ERROR|D_MAKEFLOW_HOOK, "could not copy file %s to s3 bucket: %d %s\n", id, errno, strerror(errno));
			rv = 1;
			goto FAIL;
		}
	}
	free(file_archive_path);
	file_archive_path = string_format("../../../../files/%.2s/%s", id, id);

	/* Create a symlink to task that used/created this file. */
	int symlink_failure = symlink(file_archive_path, job_file_archive_path);
	if (symlink_failure && errno != EEXIST) {
		debug(D_ERROR|D_MAKEFLOW_HOOK, "could not create symlink %s pointing to %s: %d %s\n", 
			job_file_archive_path, file_archive_path, errno, strerror(errno));
		rv = 1;
		goto FAIL;
	}

FAIL:
	free(id);
	free(file_archive_dir);
	free(file_archive_path);
	free(job_file_archive_dir);
	return rv;
}

/* Loop over inputs and archive each file */
static int makeflow_archive_write_input_files(struct archive_instance *a, struct batch_job *t, char *archive_directory_path) {
	struct batch_file *f;

	struct list_cursor *cur = list_cursor_create(t->input_files);
	// Iterate through input files
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		char *input_file_path = string_format("%s/input_files/%s", archive_directory_path,basename(f->inner_name));
		// Archive each file for inputs
		int failed_checksum = makeflow_archive_file(a, f, input_file_path);
		free(input_file_path);
		// Check to see it was archived correctly
		if(failed_checksum){
			list_cursor_destroy(cur);
			return 0;
		}
	}
	list_cursor_destroy(cur);
	return 1;
}

/* Loop over outputs and archive each file */
static int makeflow_archive_write_output_files(struct archive_instance *a, struct batch_job *t, char *archive_directory_path) {
	struct batch_file *f;

	struct list_cursor *cur = list_cursor_create(t->output_files);
	// Iterate through output files
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		char *output_file_path = string_format("%s/output_files/%s", archive_directory_path,basename(f->inner_name));
		// Archive each file for outputs
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
	// Creates directory path to be used as string
	char *tmp_directory_path = string_format("%s%s", prefix, name);
	// Actually creates directory
	int created = create_dir(tmp_directory_path, 0777);
	free(tmp_directory_path);
	// If new directory is not created
	if (!created){
		debug(D_ERROR|D_MAKEFLOW_HOOK,"Could not create archiving directory %s\n", tmp_directory_path);
		return 1;
	}
	return 0;
}

/* Archive a batch_job.
 * Archiving requires several steps:
 *  1. Create task directory structure
 *  2. Write out task information
 *  3. Archive inputs
 *  4. Archive outputs
 *
@return 1 if archive was successful, 0 if archive failed.
 */
static int makeflow_archive_task(struct archive_instance *a, struct dag_node *n, struct batch_job *t) {
	// Generates a hash id for the task
	char *id = batch_job_generate_id(t);
	int result = 1;

	/* The archive name is binned by the first 2 characters of the id for compactness */
	char *archive_directory_path = string_format("%s/tasks/%.2s/%s", a->dir, id, id);
	debug(D_MAKEFLOW_HOOK, "archiving task %d to %s", t->taskid, archive_directory_path);

	int dir_create_error = 0;
	/* We create all the sub directories upfront for convenience */
	dir_create_error = makeflow_archive_create_dir(archive_directory_path, "/output_files/");
	dir_create_error += makeflow_archive_create_dir(archive_directory_path, "/input_files/");

	// Checks to see if there was an error creating the directories
	if(dir_create_error){
		result = 0;
		goto FAIL;
	}

	/* Log the task info in the task directory */
	if(!makeflow_archive_write_task_info(a, n, t, archive_directory_path)){
		result = 0;
		goto FAIL;
	}

	// Create the input files
	if(!makeflow_archive_write_input_files(a, t, archive_directory_path)){
		result = 0;
		goto FAIL;
	}
	// Create the output files
	if(!makeflow_archive_write_output_files(a, t, archive_directory_path)){
		result = 0;
		goto FAIL;
	}

	printf("task %d successfully archived\n", t->taskid);

FAIL:
	// Free all of the memory
	free(archive_directory_path);
	free(id);
	return result;
}


/* Remove partial or corrupted archive.
@return 1 if archive was successful, 0 if archive failed.
 */
static int makeflow_archive_remove_task(struct archive_instance *a, struct dag_node *n, struct batch_job *t) {
	/* Generate the task id */
	char *id = batch_job_generate_id(t);

	/* The archive name is binned by the first 2 characters of the id for compactness */
	char *archive_directory_path = string_format("%s/tasks/%.2s/%s", a->dir, id, id);
	debug(D_MAKEFLOW_HOOK, "removing corrupt archive for task %d at %s", t->taskid, archive_directory_path);
	free(id);

	if(!unlink_recursive(archive_directory_path)){
		debug(D_MAKEFLOW_HOOK, "unable to remove corrupt archive for task %d", t->taskid);
		free(archive_directory_path);
		return 0;
	}

	debug(D_MAKEFLOW_HOOK, "corrupt archive for task %d removed", t->taskid);

	free(archive_directory_path);
	return 1;
}

int makeflow_archive_copy_preserved_files(struct archive_instance *a, struct batch_job *t, char *task_path ) {
	struct batch_file *f;
	struct stat buf;
	struct list_cursor *cur = list_cursor_create(t->output_files);
	// Iterate through output files
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		char *file_name = xxstrdup(f->outer_name);
		debug(D_MAKEFLOW_HOOK,"Trying to copy file to %s",file_name);
		char *file_to_check = xxstrdup(file_name);
		//Check to see if the directory was copied as an empty file/incorrectly
		stat(dirname(file_to_check),&buf);
		if(S_ISREG(buf.st_mode)){
			debug(D_MAKEFLOW,"Removing empty file in the place of directory name %s",file_to_check);
			char *dirEmpty = string_format("rm -rf %s",file_to_check);
			system(dirEmpty);
			free(dirEmpty);
		}
		free(file_to_check);
		// Gets path of output file
		char *output_file_path = string_format("%s/output_files/%s",task_path,basename(file_name));
		char *directory_name = xxstrdup(file_name);
		debug(D_MAKEFLOW_HOOK,"Creating directory %s",dirname(directory_name));
		if(strcmp(directory_name,file_name) != 0){
			//Create the upper level directory to copy the output files into if necessary
			if (!create_dir(directory_name, 0777) && errno != EEXIST){
				debug(D_ERROR|D_MAKEFLOW_HOOK,"Failed to create directory %s",directory_name);
				free(directory_name);
				free(output_file_path);
				free(file_name);
				return 1;
			}
		}
		free(directory_name);
		// Copy output file or directory over to specified location
		if(path_is_dir(output_file_path) != 1){
			int64_t success = copy_file_to_file(output_file_path, file_name);
			free(output_file_path);
			free(file_name);
			if (success < 0) {
				list_cursor_destroy(cur);
				debug(D_ERROR|D_MAKEFLOW_HOOK,"Failed to copy output file %s to %s\n", output_file_path, file_name);
				return 1;
			}
		}
		else{
			if(copy_dir(output_file_path, file_name) != 0){
				list_cursor_destroy(cur);
								debug(D_ERROR|D_MAKEFLOW_HOOK,"Failed to copy output file %s to %s\n", output_file_path, file_name);
				free(output_file_path);
							free(file_name);
								return 1;
						}
			free(output_file_path);
			free(file_name);
		}
		}


	list_cursor_destroy(cur);

	return 0;
}

int makeflow_archive_is_preserved(struct archive_instance *a, struct batch_job *t, char *task_path) {
	struct batch_file *f;
	struct stat buf;
	// If the task does NOT adhere to the sandbox or there is a failure with getting the stat
	if(makeflow_archive_task_adheres_to_sandbox(t) || (stat(task_path, &buf) < 0)){
		/* Not helpful unless you know the task number. */
		debug(D_MAKEFLOW_HOOK, "task %d has not been previously archived at %s", t->taskid, task_path);
		return 0;
	}

	struct list_cursor *cur = list_cursor_create(t->output_files);
	// Iterate through output files making sure they all exist
	for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
		// Get path of the output file
		char *filename = string_format("%s/output_files/%s", task_path, basename(f->inner_name));
		// Check the statistics of the output file at the location
		int file_exists = stat(filename, &buf);
		// If there is a failure with running stat delete the cursor and free memory
		if (file_exists < 0) {
			list_cursor_destroy(cur);
			// Print debug error
			debug(D_MAKEFLOW_HOOK, "output file %s not found in archive at %s: %d %s", 
				f->outer_name, filename, errno, strerror(errno));
			free(filename);
			return 0;
		}
		free(filename);
	}
	// Free list cursor memory
	list_cursor_destroy(cur);

	return 1;
}

static int makeflow_s3_archive_copy_task_files(struct archive_instance *a, char *id, char *task_path, struct batch_job *t){
	char *taskTarFile = string_format("%s/%s",task_path,id);
	// Check to see if the task is already in the local archive so it is not downloaded twice
	if(access(taskTarFile,R_OK) != 0){
		// Copy tar file from the s3 bucket
		struct timeval start_time;
		struct timeval end_time;
		char *copyTar = string_format("%s/%s",task_path,id);
		FILE *taskFile = fopen(copyTar,"wb");
		gettimeofday(&start_time,NULL);
		if(s3_get(taskFile,id) != 0){
			gettimeofday(&end_time,NULL);
					float run_time = ((end_time.tv_sec*1000000 + end_time.tv_usec) - (start_time.tv_sec*1000000 + start_time.tv_usec)) / 1000000.0;
					total_down_time += run_time;
					debug(D_MAKEFLOW_HOOK," It took %f seconds for %s to fail downloading to %s",run_time, id, a->s3_dir);
					debug(D_MAKEFLOW_HOOK," The total download time is %f second(s)",total_down_time);
			free(copyTar);
			return 0;
		}
		gettimeofday(&end_time,NULL);
				float run_time = ((end_time.tv_sec*1000000 + end_time.tv_usec) - (start_time.tv_sec*1000000 + start_time.tv_usec)) / 1000000.0;
				total_down_time += run_time;
		printf("Download %s from %s/%s\n",id,a->s3_dir,id);
				debug(D_MAKEFLOW_HOOK," It took %f seconds for %s to download from %s",run_time, id, a->s3_dir);
				debug(D_MAKEFLOW_HOOK," The total download time is %f second(s)",total_down_time);
		free(copyTar);
		fclose(taskFile);

		char *extractTar = string_format("tar -xzvf %s/%s -C %s",task_path,id,task_path);
		if(system(extractTar) == -1){
			free(extractTar);
			return 0;
		}
		free(extractTar);

		struct batch_file *f;
		struct list_cursor *cur = list_cursor_create(t->output_files);
		// Iterate through output files
		for(list_seek(cur, 0); list_get(cur, (void**)&f); list_next(cur)) {
			char *output_file_path = string_format("%s/output_files/%s", task_path,  basename(f->inner_name));
			char buf[1024];
			ssize_t len;
			// Read what the symlink is actually pointing to
			if((len = readlink(output_file_path, buf, sizeof(buf)-1)) != -1)
				buf[len] = '\0';
			free(output_file_path);
			// Grabs the actual name of the file from the buffer
			char *file_name	= basename(buf);
			debug(D_MAKEFLOW_HOOK,"The FILE_NAME  is %s",file_name);
			// Check to see if the file was already copied to the /files/ directory
			char *filePath = string_format("%s/files/%.2s/%s",a->dir,file_name,file_name);
			char *fileDir = string_format("%s/files/%.2s",a->dir,file_name);
			if(access(filePath,R_OK) != 0){
				debug(D_MAKEFLOW_HOOK,"COPYING  %s to /files/ from the s3 bucket",file_name);
				// Copy the file to the local archive /files/ directory
				gettimeofday(&start_time,NULL);
				create_dir(fileDir,0777);
				FILE *fileLocal = fopen(filePath, "wb");
				if(s3_get(fileLocal, file_name) != 0){
					gettimeofday(&end_time,NULL);
							run_time = ((end_time.tv_sec*1000000 + end_time.tv_usec) - (start_time.tv_sec*1000000 + start_time.tv_usec)) / 1000000.0;
							total_down_time += run_time;
							debug(D_MAKEFLOW_HOOK," It took %f seconds for %s to fail downloading from %s",run_time, id, a->s3_dir);
							debug(D_MAKEFLOW_HOOK," The total download time is %f second(s)",total_down_time);
					return 0;
				}
				gettimeofday(&end_time,NULL);
						run_time = ((end_time.tv_sec*1000000 + end_time.tv_usec) - (start_time.tv_sec*1000000 + start_time.tv_usec)) / 1000000.0;
						total_down_time += run_time;
				printf("Download %s from %s/%s\n",file_name,a->s3_dir, file_name);
						debug(D_MAKEFLOW_HOOK," It took %f seconds for %s to download from %s",run_time, id, a->s3_dir);
						debug(D_MAKEFLOW_HOOK," The total download time is %f second(s)",total_down_time);
				fclose(fileLocal);
				//Extract the tar file of a directory (always run even if it isnt a tar file)
				char *extractDirTar = string_format("tar -xzvf %s -C %s/foo >&/dev/null",filePath,fileDir);
				char *makeDir = string_format("mkdir %s/foo",fileDir);
				system(makeDir);
				free(makeDir);
				if(system(extractDirTar) != 0){
					debug(D_MAKEFLOW_HOOK,"%s is either a file or the tar file could not be extracted",file_name);
					free(extractDirTar);
					char *removeFooDir = string_format("rm -rf %s/foo",fileDir);
					system(removeFooDir);
					continue;
				}
				char *removeTar = string_format("rm %s",filePath);
				system(removeTar);
				free(removeTar);
				char *renameFile = string_format("mv %s/foo %s", fileDir, filePath);
				system(renameFile);
				free(renameFile);
				free(extractDirTar);

			}
			free(fileDir);
			free(filePath);
		}
		free(taskTarFile);
		return 1;
	}
	debug(D_MAKEFLOW_HOOK,"TASK already exist in local archive, not downloading from s3 bucket");
	free(taskTarFile);
	return 1;
}

static int batch_submit( void * instance_struct, struct batch_job *t){
	struct archive_instance *a = (struct archive_instance*)instance_struct;
	int rc = MAKEFLOW_HOOK_SUCCESS;
	// Generates a hash id for the task
	char *id = batch_job_generate_id(t);
	char *task_path = string_format("%s/tasks/%.2s/%s",a->dir, id, id);
	create_dir(task_path,0777);
	debug(D_MAKEFLOW_HOOK, "Checking archive for task %d at %.5s\n", t->taskid, id);
	if(a->s3){
		int result = 1;
		result = makeflow_s3_archive_copy_task_files(a, id, task_path, t);
		if(!result){
			debug(D_MAKEFLOW_HOOK, "unable to copy task files for task %s  from S3 bucket",id);
		}

	}

	// If a is in read mode and the archive is preserved (all the output files exist)
	if(a->read && makeflow_archive_is_preserved(a, t, task_path)){
		debug(D_MAKEFLOW_HOOK, "Task %d already exists in archive, replicating output files\n", t->taskid);

		/* copy archived files to working directory and update state for node and dag_files */
		makeflow_archive_copy_preserved_files(a, t, task_path);
		t->info->exited_normally = 1;
		a->found_archived_job = 1;
		printf("task %d was pulled from archive\n", t->taskid);
		rc = MAKEFLOW_HOOK_SKIP;
	}

	free(id);
	free(task_path);
	return rc;
}

static int batch_retrieve( void * instance_struct, struct batch_job *t){
	struct archive_instance *a = (struct archive_instance*)instance_struct;
	int rc = MAKEFLOW_HOOK_SUCCESS;
	// Generates a hash id for the task
	char *id = batch_job_generate_id(t);
	char *task_path = string_format("%s/tasks/%.2s/%s",a->dir, id, id);

	// If a is in read mode and the archive is preserved (all the output files exist)
	if(a->read && makeflow_archive_is_preserved(a, t, task_path)){
		// Print out debug statement
		debug(D_MAKEFLOW_HOOK, "Task %d run was bypassed using archive\n", t->taskid);
		// Bypass task run
		rc = MAKEFLOW_HOOK_RUN;
	}
	// Free excess memory
	free(id);
	free(task_path);
	return rc;
}

/*Compress the task file and copy it to the S3 bucket*/
static int makeflow_archive_s3_task(struct archive_instance *a, char *taskID, char *task_path){
	// Convert directory to a tar.gz file
	struct timeval start_time;
		struct timeval end_time;
	char *tarConvert = string_format("tar -czvf %s.tar.gz -C %s .",taskID,task_path);
	if(system(tarConvert) == -1){
		free(tarConvert);
		return 0;
	}
	free(tarConvert);

	// Add file to the s3 bucket
	char *tarFile = string_format("%s.tar.gz",taskID);
	FILE *fp = fopen(tarFile,"rb");
		gettimeofday(&start_time, NULL);
	if(s3_put(fp,taskID) != 0){
		gettimeofday(&end_time,NULL);
				float run_time = ((end_time.tv_sec*1000000 + end_time.tv_usec) - (start_time.tv_sec*1000000 + start_time.tv_usec)) / 1000000.0;
				total_up_time += run_time;
				debug(D_MAKEFLOW_HOOK," It took %f seconds for %s to fail uploading to %s",run_time, taskID, a->s3_dir);
				debug(D_MAKEFLOW_HOOK," The total upload time is %f second(s)",total_up_time);
		free(tarFile);
		return 0;
	}
	gettimeofday(&end_time,NULL);
		float run_time = ((end_time.tv_sec*1000000 + end_time.tv_usec) - (start_time.tv_sec*1000000 + start_time.tv_usec)) / 1000000.0;
		total_up_time += run_time;
	printf("Upload %s to %s/%s\n",tarFile,a->s3_dir,taskID);
		debug(D_MAKEFLOW_HOOK," It took %f seconds for %s to upload to %s",run_time, taskID, a->s3_dir);
		debug(D_MAKEFLOW_HOOK," The total upload time is %f second(s)",total_up_time);
	fclose(fp);
	// Remove extra tar files on local directory
	char *removeTar = string_format("rm %s",tarFile);
	if(system(removeTar) == -1){
		free(removeTar);
		return 0;
	}
	free(tarFile);
	free(removeTar);

	return 1;
}
static int node_success( void * instance_struct, struct dag_node *n, struct batch_job *t){
	struct archive_instance *a = (struct archive_instance*)instance_struct;
	/* store node into archiving directory  */
	// If a is in write mode
	if (a->write) {
		// If the task does NOT adhere to the sandbox
		if(makeflow_archive_task_adheres_to_sandbox(t)){
			// Print debug error message
			debug(D_ERROR|D_MAKEFLOW_HOOK, "task %d will not be archived", t->taskid);
			return MAKEFLOW_HOOK_SUCCESS;
		}

		// Generates a hash id for the task
		char *id = batch_job_generate_id(t);
		char *task_path = string_format("%s/tasks/%.2s/%s",a->dir, id, id);
		// If the archive is preserved (all the output files exist)
		if(makeflow_archive_is_preserved(a, t, task_path)){
			// Free excess memory
			free(id);
			free(task_path);
			debug(D_MAKEFLOW_HOOK, "Task %d already exists in archive", t->taskid);
			return MAKEFLOW_HOOK_SUCCESS;
		}

		// Otherwise archive the task
		debug(D_MAKEFLOW_HOOK, "archiving task %d in directory: %s\n",t->taskid, a->dir);
		int archived = makeflow_archive_task(a, n, t);
		if(!archived){
			debug(D_MAKEFLOW_HOOK, "unable to archive task %d in directory: %s\n",t->taskid, a->dir);
			makeflow_archive_remove_task(a, n, t);
			return MAKEFLOW_HOOK_FAILURE;
		}
		debug(D_MAKEFLOW_HOOK,"The task ID in node_success is %s",id);
		if(a->s3){
			int s3Archived = 1;
			// Check to see if the task  is already in the s3 bucket
			if(a->s3_check){
				if(!in_s3_archive(a,id))
					s3Archived = makeflow_archive_s3_task(a,id, task_path);
			}
			else
				s3Archived = makeflow_archive_s3_task(a,id, task_path);
			if(!s3Archived){
				debug(D_MAKEFLOW_HOOK, "unable to archive task %s in S3 archive",id);
				return MAKEFLOW_HOOK_FAILURE;
			}
		}

		free(id);
		free(task_path);
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


