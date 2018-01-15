/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BATCH_FILE_H
#define BATCH_FILE_H

#include "batch_job.h"
#include "list.h"

typedef enum {
    BATCH_FILE_INCLUDE,   /* This file is included in files string. (Default) */
    BATCH_FILE_EXCLUDE,   /* This file is excluded from files string. */
} batch_file_inclusion_t;

struct batch_file {
	char *host_name;
	char *exe_name;
	batch_file_inclusion_t batch_sys_include;
};

/** Create batch_file struct.
@param queue The batch_queue that this file is being created for. 
@param host_name A pointer to the file's name host/submission side.
@param exe_name A pointer to the file's name execution side.
@return batch_file struct.
*/
struct batch_file *batch_file_create(struct batch_queue *queue, char *host_name, char *exe_name );

/** Delete batch_file struct.
 This includes freeing host_name and exe_name if defined.
@param file A batch_file struct to be deleted.
*/
void batch_file_delete(struct batch_file *f);

/** Flatten batch_file to string.
@param queue The batch_queue that this is being flattened for. 
@param file A batch_file struct to be stringified. 
@return pointer to char * representing the flattened list.
*/
char * batch_file_to_string(struct batch_queue *queue, struct batch_file *f );

/** Flatten batch_file list to string.
@param files A list struct containing batch_file structs. 
@param queue The batch_queue that this is being flattened for. 
@return pointer to char * representing the flattened list.
*/
char * batch_files_to_string(struct batch_queue *queue, struct list *files );

/** Signal that this batch_file is ignored by batch_queue. 
 Used in batch_file_to_string to exclude file from string list, but
 acknowledge the necessity of this file.
@param file The batch_file to be ignored/enabled.
@param ignored If BATCH_FILE_INCLUDE this file will be included in files string,
	if BATCH_FILE_EXCLUDE this file is ignored for files string.
*/
void batch_file_inclusion(struct batch_file *f, batch_file_inclusion_t inclusion);

#endif
