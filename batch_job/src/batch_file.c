/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_file.h"
#include "stringtools.h"
#include "xxmalloc.h"

/**
 * Create batch_file from host_name and exe_name.
 * Host name indicates the name that will be on the host/submission side.
 *  This is equivalent to the filename in Makeflow.
 * Exe name indicates the name that will be used for execution.
 *  IF no exe_name is given, or the specified batch_queue does not support
 *  remote renaming the host_name will be used.
 **/
struct batch_file *batch_file_create(struct batch_queue *queue, char * host_name, char * exe_name)
{
	struct batch_file *f = malloc(sizeof(struct batch_file));
    f->host_name = xxstrdup(host_name);

	if(batch_queue_supports_feature(queue, "remote_rename") && exe_name){
		f->exe_name = xxstrdup(exe_name);
	} else {
		f->exe_name = f->host_name;
	}

	f->batch_sys_include = BATCH_FILE_INCLUDE;

    return f;
}

/**
 * Delete batch_file, including freeing host_name and exe_name/
 **/
void batch_file_delete(struct batch_file *f)
{
	free(f->host_name);
	f->host_name = NULL;
	if(f->exe_name)
		free(f->exe_name);

	free(f);
}

/**
 * Given a file, return the string that identifies it appropriately
 * for the given batch system, combining the local and remote name
 * and making substitutions according to the node.
 **/
char * batch_file_to_string(struct batch_queue *queue, struct batch_file *f )
{
	if(f->batch_sys_include == BATCH_FILE_EXCLUDE) return strdup("");	

    if(batch_queue_supports_feature(queue,"remote_rename")) {
            return string_format("%s=%s,", f->host_name, f->exe_name);
    } else {
            return string_format("%s,", f->host_name);
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

    if(!files) return file_str;

    list_first_item(files);
    while((file=list_next_item(files))) {
        char *f = batch_file_to_string(queue, file);
        file_str = string_combine(file_str,f);
        free(f);
    }

    return file_str;
}

/**
 * Indicate if file is to be included or not in files string
 **/
void batch_file_inclusion(struct batch_file *f, batch_file_inclusion_t inclusion)
{
	f->batch_sys_include = inclusion;
}

