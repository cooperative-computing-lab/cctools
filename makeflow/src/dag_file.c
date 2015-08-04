/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "dag.h"
#include "dag_file.h"

#include "xxmalloc.h"
#include "list.h"

#include <stdlib.h>

struct dag_file * dag_file_create( const char *filename )
{
	struct dag_file *f = malloc(sizeof(*f));
	f->filename = xxstrdup(filename);
	f->needed_by = list_create();
	f->created_by = 0;
	f->ref_count = 0;
	f->state = DAG_FILE_STATE_UNKNOWN;
	return f;
}

const char *dag_file_state_name(dag_file_state_t state)
{
	switch (state) {
	case DAG_FILE_STATE_UNKNOWN:
		return "waiting";
	case DAG_FILE_STATE_EXPECT:
		return "running";
	case DAG_FILE_STATE_EXISTS:
		return "receive";
	case DAG_FILE_STATE_COMPLETE:
		return "complete";
	case DAG_FILE_STATE_DELETE:
		return "delete";
	case DAG_FILE_STATE_DOWN:
		return "download";
	case DAG_FILE_STATE_UP:
		return "upload";
	default:
		return "unknown";
	}
}

int dag_file_is_source( const struct dag_file *f )
{
	if(f->created_by)
		return 0;
	else
		return 1;
}

int dag_file_is_sink( const struct dag_file *f )
{
	if(list_size(f->needed_by) > 0)
		return 0;
	else
		return 1;
}

int dag_file_exists( const struct dag_file *f )
{
	if(f->state == DAG_FILE_STATE_EXISTS
		|| f->state == DAG_FILE_STATE_COMPLETE
		|| dag_file_is_source(f))
		return 1;
	else
		return 0;
}

int dag_file_in_trans( const struct dag_file *f )
{
	if(f->state == DAG_FILE_STATE_EXPECT
		|| f->state == DAG_FILE_STATE_DOWN
		|| f->state == DAG_FILE_STATE_UP)
		return 1;
	else
		return 0;
}

/* vim: set noexpandtab tabstop=4: */
