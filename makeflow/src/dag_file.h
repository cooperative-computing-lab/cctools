/*
Copyright (C) 2014- The University of Notre Dame
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
	DAG_FILE_STATE_UNKNOWN = 0,
	DAG_FILE_STATE_EXPECT = 1,
	DAG_FILE_STATE_EXISTS = 2,
	DAG_FILE_STATE_COMPLETE = 3,
	DAG_FILE_STATE_DELETE = 4,
	DAG_FILE_STATE_DOWN = 5,
	DAG_FILE_STATE_UP = 6
} dag_file_state_t;


struct dag_file {
	const char *filename;
	struct list     *needed_by;              /* List of nodes that have this file as a source */
	struct dag_node *created_by;             /* The node (if any) that created the file */
	int    ref_count;                        /* How many nodes still to run need this file */
	dag_file_state_t state;                  /* Enum: DAG_FILE_STATE_{INTIAL,EXPECT,...} */
};

struct dag_file *dag_file_create( const char *filename );

const char *dag_file_state_name(dag_file_state_t state);
int dag_file_is_source( const struct dag_file *f );
int dag_file_is_sink( const struct dag_file *f );
int dag_file_exists( const struct dag_file *f );
int dag_file_in_trans( const struct dag_file *f );

#endif
