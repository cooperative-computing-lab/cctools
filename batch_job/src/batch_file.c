/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_file.h"
#include "stringtools.h"
#include "xxmalloc.h"

/**
 * Create batch_file from outer_name and inner_name.
 * Outer/DAG name indicates the name that will be on the host/submission side.
 *  This is equivalent to the filename in Makeflow.
 * Inner/task name indicates the name that will be used for execution.
 *  IF no inner_name is given, or the specified batch_queue does not support
 *  remote renaming the outer_name will be used.
 **/
struct batch_file *batch_file_create(struct batch_queue *queue, const char * outer_name, const char * inner_name)
{
	struct batch_file *f = calloc(1,sizeof(*f));
    f->outer_name = xxstrdup(outer_name);

	if(batch_queue_supports_feature(queue, "remote_rename") && inner_name){
		f->inner_name = xxstrdup(inner_name);
	} else {
		f->inner_name = xxstrdup(outer_name);
	}

    return f;
}

/**
 * Delete batch_file, including freeing outer_name and inner_name/
 **/
void batch_file_delete(struct batch_file *f)
{
	if(!f)
		return;

	free(f->outer_name);
	free(f->inner_name);

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
            return string_format("%s=%s", f->outer_name, f->inner_name);
    } else {
            return string_format("%s", f->outer_name);
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

	char * separator = "";

    if(!files) return file_str;

    list_first_item(files);
    while((file=list_next_item(files))) {
		/* Only add separator if past first item. */
		file_str = string_combine(file_str,separator);

        char *f = batch_file_to_string(queue, file);
        file_str = string_combine(file_str,f);
	
		/* This could be set using batch_queue feature or option 
		 * to allow for batch system specific separators. */
		separator = ",";

        free(f);
    }

    return file_str;
}

