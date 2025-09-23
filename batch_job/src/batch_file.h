/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BATCH_FILE_H
#define BATCH_FILE_H

#include <errno.h>
#include <sys/types.h>
#include <dirent.h>

#include "sha1.h"
#include "list.h"
#include "debug.h"
#include "path.h"

/** @file batch_file.h  Describes a single input or output file of a batch job.
For each input/output file consumed/produced by a single @ref batch_job, a @ref batch_file
object describes the name of the file as the submitter sees it (outer_name) and
the intended name of the file as seen by the running job (inner_name).  Many (but not all)
batch systems execute jobs in a sandbox that permits these two names to be different.
Some batch systems do not permit these names to differ.
*/

/** Internal description of a single file used by a batch job. */
struct batch_file {
	char *outer_name;   /**< The name of the file in the submitters filesystem namespace. */
	char *inner_name;   /**< The name of the file as it should appear to the running job. */
	char *hash;         /**< The hierarchical checksum of this file/directory, when content based names are used. */
};

/** Create batch_file struct.
@param outer_name A pointer to the file's name host/submission side.
@param inner_name  A pointer to the file's name execution side.
@return batch_file struct.
*/
struct batch_file *batch_file_create(const char *outer_name, const char *inner_name );

/** Delete batch_file struct.
This includes freeing host_name and exe_name if defined.
@param file A batch_file struct to be deleted.
*/
void batch_file_delete( struct batch_file *file );

/** Output batch_file as a string.
* Format is "outer_name=inner_name" when renaming is needed.
* and just "outer_name" when it is not.
* @param file A batch_file struct to be stringified. 
* @return pointer to char * representing the flattened list.
*/

char * batch_file_to_string( struct batch_file *file );

/** Output list of batch_files as a string.
* Format is "FILE,FILE,...,FILE" where file is the result of batch_file_to_string.
* @param files A list struct containing batch_file structs. 
* @return pointer to char * representing the flattened list.
*/
char * batch_file_list_to_string( struct list *files );

/** Compare function for comparing batch_files based on outer_name.
@param file1 First file to compare.
@param file2 Second file to compare.
@return Relative alphabetic order of files outer_name's
*/
int batch_file_outer_compare( struct batch_file *file1, struct batch_file *file2 );

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
