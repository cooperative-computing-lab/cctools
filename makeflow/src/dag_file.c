/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "dag.h"
#include "dag_file.h"

#include "xxmalloc.h"
#include "list.h"
#include "macros.h"

#include <stdlib.h>

struct dag_file * dag_file_create( const char *filename )
{
	struct dag_file *f = malloc(sizeof(*f));
	f->filename = xxstrdup(filename);
	f->needed_by = list_create();
	f->created_by = 0;
	f->actual_size = 0;
	f->estimated_size = GIGABYTE;
	f->reference_count = 0;
	f->state = DAG_FILE_STATE_UNKNOWN;
	f->type = DAG_FILE_TYPE_INTERMEDIATE;
	f->source = NULL;
	f->cache_name = NULL;
	f->source_type = DAG_FILE_SOURCE_LOCAL;
	f->cache_id = NULL;
	return f;
}

/* Converts enum to string value for decoding file state */
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

/* Returns whether the file is created in the DAG or not */
int dag_file_is_source( const struct dag_file *f )
{
	if(f->created_by)
		return 0;
	else
		return 1;
}

/* Returns whether the file is used in any rule */
int dag_file_is_sink( const struct dag_file *f )
{
	if(list_size(f->needed_by) > 0)
		return 0;
	else
		return 1;
}

/* Reports if a file is expeced to exist, does not guarantee existence
 * if files are altered outside of Makeflow */
int dag_file_should_exist( const struct dag_file *f )
{
	if(f->state == DAG_FILE_STATE_EXISTS
		|| f->state == DAG_FILE_STATE_COMPLETE
		|| dag_file_is_source(f))
		return 1;
	else
		return 0;
}

/* Reports if a file is in the process of being created, downloaded,
 * or uploaded. As in file in transition */
int dag_file_in_trans( const struct dag_file *f )
{
	if(f->state == DAG_FILE_STATE_EXPECT
		|| f->state == DAG_FILE_STATE_DOWN
		|| f->state == DAG_FILE_STATE_UP)
		return 1;
	else
		return 0;
}

/* If the file exists, return actual size, else return estimated
 * size. In the default case that size is 1GB */
uint64_t dag_file_size( const struct dag_file *f )
{
	if(dag_file_should_exist(f))
		return f->actual_size;
	return f->estimated_size;
}

/* Returns the sum of results for dag_file_size for each file
 * in list. */
uint64_t dag_file_list_size(struct list *s)
{
	struct dag_file *f;
	uint64_t size = 0;
	list_first_item(s);
	while((f = list_next_item(s)))
		size += dag_file_size(f);

	return size;
}

/* Returns the sum of results for dag_file_size for each file
 * in set. */
uint64_t dag_file_set_size(struct set *s)
{
	struct dag_file *f;
	uint64_t size = 0;
	set_first_element(s);
	while((f = set_next_element(s)))
		size += dag_file_size(f);

	return size;
}

int dag_file_coexist_files(struct set *s, struct dag_file *f)
{
	struct dag_node *n;
	list_first_item(f->needed_by);
	while((n = list_next_item(f->needed_by))){
		if(set_lookup(s, n))
			return 1;
	}
	return 0;
}

void dag_file_mount_clean(struct dag_file *df) {
	if(!df) return;

	if(df->source) {
		free(df->source);
		df->source = NULL;
	}

	if(df->cache_name) {
		free(df->cache_name);
		df->cache_name = NULL;
	}
}

/* vim: set noexpandtab tabstop=4: */
