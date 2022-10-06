/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BATCH_FILE_H
#define BATCH_FILE_H
#include <errno.h>
#include <sys/types.h>
#include <dirent.h>

#include "batch_job.h"
#include "sha1.h"
#include "list.h"
#include "debug.h"
#include "path.h"


struct batch_file {
	char *outer_name;
	char *inner_name;
	char *hash;/* Checksum based on contents. */
};

/** Create batch_file struct.
@param queue The batch_queue that this file is being created for. 
@param outer_name A pointer to the file's name host/submission side.
@param inner_name  A pointer to the file's name execution side.
@return batch_file struct.
*/
struct batch_file *batch_file_create(struct batch_queue *queue, const char *outer_name, const char *inner_name );

/** Delete batch_file struct.
 This includes freeing host_name and exe_name if defined.
@param file A batch_file struct to be deleted.
*/
void batch_file_delete(struct batch_file *f);

/** Output batch_file as a string.
 Format is "outer_name=inner_name" where batch_queue supports 
 remote renaming and "outer_name" where it does not.
@param queue The batch_queue that this is being flattened for. 
@param file A batch_file struct to be stringified. 
@return pointer to char * representing the flattened list.
*/
char * batch_file_to_string(struct batch_queue *queue, struct batch_file *f );

/** Output list of batch_files as a string.
 Format is "FILE,FILE,...,FILE" where file is the result of batch_file_to_string.
@param files A list struct containing batch_file structs. 
@param queue The batch_queue that this is being flattened for. 
@return pointer to char * representing the flattened list.
*/
char * batch_files_to_string(struct batch_queue *queue, struct list *files );

/** Compare function for comparing batch_files based on outer_name.
@param file1 First file to compare.
@param file2 Second file to compare.
@return Relative alphabetic order of files outer_name's
*/ 
int batch_file_outer_compare(const void *file1, const void *file2);

/** Generate a sha1 hash based on the file contents.
@param f The batch_file whose checksum will be generated.
@return Allocated string of the hash, user should free or NULL on error of checksumming file.
*/
char * batch_file_generate_id(struct batch_file *f);

/** Generates a sha1 hash based on the directory's contents.
@param file_name The directory that will be checked
@return Allocated string of the hash, user should free or NULL on error scanning the directory.
*/
char *  batch_file_generate_id_dir(char *file_name);

#endif
