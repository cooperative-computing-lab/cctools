/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DAG_FILE_H
#define DAG_FILE_H

#include "dag.h"

/* struct dag_file represents a file, input or output, of the
 * workflow. filename is the path given in the makeflow file,
 * that is the local name of the file. Additionaly, dag_file
 * keeps track which nodes use the file as a source, and the
 * unique node, if any, that produces the file.
 */

typedef enum {
	DAG_FILE_STATE_UNKNOWN,   /* Initial State indicates the file is in DAG */
	DAG_FILE_STATE_EXPECT,    /* Rule that creates this file is in progress */
	DAG_FILE_STATE_EXISTS,    /* File has been successfully completed and is used elsewhere */
	DAG_FILE_STATE_COMPLETE,  /* File exists, but no unfinished rule needs the file */
	DAG_FILE_STATE_DELETE,    /* File was deleted as it was no longer needed, only intermediate files */
	DAG_FILE_STATE_DOWN,      /* UNUSED STATE, included for when files can be downloaded */
	DAG_FILE_STATE_UP         /* UNUSED STATE, included for when files can be uploaded */
} dag_file_state_t;


typedef enum {
	/* File types are specific to DAG files. */
	DAG_FILE_TYPE_INPUT,       /* File has no rule that creates it or is specified as input.
                                  No input files are cleaned in garbage collection */
	DAG_FILE_TYPE_OUTPUT,      /* If outputs are specified with MAKEFLOW_OUTPUTS then the specified
                                  files are this category, otherwise all sink files are included.
                                  No output files are cleaned in garbage collection */
	DAG_FILE_TYPE_INTERMEDIATE,/* Files that are created and used in DAG, but can be deleted */
	/* File types that are specific to hook/wrapper. */
	DAG_FILE_TYPE_TEMP,        /* File created for node that should be removed after completion.
                                  If node fails, should be moved to fail dir. */
	DAG_FILE_TYPE_GLOBAL       /* File that exists prior to DAG. Is not logged otherwise future
                                  invocations of Makeflow will clean it. */
} dag_file_type_t;

/* the type of a dependency specified in the mountfile */
typedef enum {
	DAG_FILE_SOURCE_LOCAL,
	DAG_FILE_SOURCE_HTTP,
	DAG_FILE_SOURCE_HTTPS,
	DAG_FILE_SOURCE_UNSUPPORTED
} dag_file_source_t;

struct dag_file {
	const char *filename;
	struct list     *needed_by;     /* List of nodes that have this file as a source */
	struct dag_node *created_by;    /* The node (if any) that created the file */
	uint64_t actual_size;           /* File size reported by stat */
	uint64_t estimated_size;        /* File size estimation provided prior to execution */
	int    reference_count;         /* How many nodes still to run need this file */
	time_t creation_logged;         /* Time that file creation is logged */
	dag_file_state_t state;         /* Enum: DAG_FILE_STATE_{INTIAL,EXPECT,...} */
	dag_file_type_t type;           /* Enum: DAG_FILE_TYPE_{INPUT,...} */
	char *source;                   /* the source of the file specified in the mountfile, by default is NULL */
	char *cache_name;               /* the name of a file dependency in the cache, by default is NULL */
	dag_file_source_t source_type;  /* the type of the source of a dependency */
	char *hash;                     /* the hash computed based on the files contents */
};

/** Create dag file struct.
@param filename A const pointer to the unique filename.
@return dag_file struct.
*/
struct dag_file *dag_file_create( const char *filename );

/** Create JX object of file struct.
Contains dag_name (originally filename, will be outer_name in code), 
task_name(originally remote_filename, will be inner_name in code), 
and size if defined.
* @param f dag_file.
* @param n dag_node to provide context for remote names.
* @return JX object of file.
*/
struct jx * dag_file_to_jx( struct dag_file *f, struct dag_node *n);

/** Returns the string defining the files state, intended for logging.
@param Enum DAG_FILE_STATE_* for the printable name of the state.
@return Const char of the name.
*/
const char *dag_file_state_name(dag_file_state_t state);

/** Boolean to expressing if the file is created by DAG.
@param f dag_file.
@return Zero if created by DAG, one if not.
*/
int dag_file_is_source( const struct dag_file *f );

/** Boolean to expressing if the file is used by DAG.
@param f dag_file.
@return Zero if used by DAG, one if not.
*/
int dag_file_is_sink( const struct dag_file *f );

/** Boolean if the file is expected to exist, based on dag_file_state.
@param f dag_file.
@return One if expected to exist, zero if not.
*/
int dag_file_should_exist( const struct dag_file *f );

/** Boolean if the file is in transit, based on dag_file_state. UNUSED.
@param f dag_file.
@return One if in transit state, zero if not.
*/
int dag_file_in_trans( const struct dag_file *f );
uint64_t dag_file_size( const struct dag_file *f );

/** Report the size of file. If no actual size exists, estimated size will be used.
@param f dag_file.
@return Size of file.
*/
uint64_t dag_file_size( const struct dag_file *f );

/** Report the sum of file sizes in list. Estimated size is used if actual 
does not exist.
@param s Pointer to list of dag_files.
@return Sum of dag_file sizes.
*/
uint64_t dag_file_list_size(struct list *s);

/** Report the sum of file sizes in set. Estimated size is used if actual 
does not exist.
@param s Pointer to set of dag_files.
@return Sum of dag_file sizes.
*/
uint64_t dag_file_set_size(struct set *s);

/** Given a f and set of nodes, determine if that file is used in that 
set of nodes.
@param s Pointer to set of dag_nodes.
@param f dag_file.
@return One if used, zero if not.
*/
int dag_file_coexist_files(struct set *s, struct dag_file *f);
/* dag_file_mount_clean cleans up the mem space allocated for dag_file due to the usage of mountfile
 */
void dag_file_mount_clean( struct dag_file *df );

#endif
