/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_file.h"
#include "stringtools.h"
#include "xxmalloc.h"

/**
 * Create batch_file from name_on_submission and name_on_execution.
 * Host name indicates the name that will be on the host/submission side.
 *  This is equivalent to the filename in Makeflow.
 * Exe name indicates the name that will be used for execution.
 *  IF no name_on_execution is given, or the specified batch_queue does not support
 *  remote renaming the name_on_submission will be used.
 **/
struct batch_file *batch_file_create(struct batch_queue *queue, char * name_on_submission, char * name_on_execution)
{
	struct batch_file *f = malloc(sizeof(*f));
    f->name_on_submission = xxstrdup(name_on_submission);

	if(batch_queue_supports_feature(queue, "remote_rename") && name_on_execution){
		f->name_on_execution = xxstrdup(name_on_execution);
	} else {
		f->name_on_execution = xxstrdup(name_on_submission);
	}

	f->batch_sys_include = BATCH_FILE_INCLUDE;

    return f;
}

/**
 * Delete batch_file, including freeing name_on_submission and name_on_execution/
 **/
void batch_file_delete(struct batch_file *f)
{
	free(f->name_on_submission);
	free(f->name_on_execution);

	free(f);
}

/**
 * Given a file, return the string that identifies it appropriately
 * for the given batch system, combining the local and remote name
 * and making substitutions according to the node.
 **/
char * batch_file_to_string(struct batch_queue *queue, struct batch_file *f )
{
    if(batch_queue_supports_feature(queue,"remote_rename")) {
            return string_format("%s=%s", f->name_on_submission, f->name_on_execution);
    } else {
            return string_format("%s", f->name_on_submission);
    }
}

/**
 * Given a list of files, add the files to the given string.
 * Returns the original string, realloced if necessary
 **/
char * batch_files_to_string(struct batch_queue *queue, struct list *files )
{
    struct batch_file *file;

    char * file_str = strdup("");

	/* This could be set using batch_queue feature or option 
	 * to allow for batch system specific separators. */
	char * separator = ",";
	int location = 0;

    if(!files) return file_str;

    list_first_item(files);
    while((file=list_next_item(files))) {
		/* This file was set to be excluded from file list. */
		if(file->batch_sys_include == BATCH_FILE_EXCLUDE)
			continue;

		/* Only add separator if past first item. */
		if(location == 0)
			file_str = string_combine(file_str,separator);

        char *f = batch_file_to_string(queue, file);
        file_str = string_combine(file_str,f);
		location++;
        free(f);
    }

    return file_str;
}

/**
 * Set if file is to be included or not in files string
 **/
void batch_file_set_inclusion_mode(struct batch_file *f, batch_file_inclusion_t inclusion)
{
	f->batch_sys_include = inclusion;
}

