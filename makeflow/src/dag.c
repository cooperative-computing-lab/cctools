/*
Copyright (C) 2014 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>

#include "debug.h"
#include "xxmalloc.h"

#include "itable.h"
#include "hash_table.h"
#include "list.h"
#include "set.h"
#include "stringtools.h"
#include "rmsummary.h"

#include "dag.h"
#include "dag_resources.h"


struct dag_variable *dag_variable_create(const char *name, const char *initial_value);

struct dag *dag_create()
{
	struct dag *d = malloc(sizeof(*d));

	memset(d, 0, sizeof(*d));
	d->nodes = 0;
	d->filename = NULL;
	d->node_table = itable_create(0);
	d->local_job_table = itable_create(0);
	d->remote_job_table = itable_create(0);
	d->files = hash_table_create(0, 0);
	d->inputs = set_create(0);
	d->outputs = set_create(0);
	d->nodeid_counter = 0;
	d->export_vars  = set_create(0);
	d->special_vars = set_create(0);
	d->completed_files = 0;
	d->deleted_files = 0;

	d->categories   = hash_table_create(0, 0);
	d->default_category = makeflow_category_lookup_or_create(d, "default");

	d->allocation_mode = CATEGORY_ALLOCATION_MODE_FIXED;
	d->cache_dir = NULL;

	d->archive_directory = ARCHIVING_DEFAULT_DIRECTORY;
	d->should_read_archive = 0;
	d->should_write_to_archive = 0;

	/* Add GC_*_LIST to variables table to ensure it is in
	 * global DAG scope. /
	hash_table_insert(d->variables,"GC_PRESERVE_LIST"   , dag_variable_create(NULL, ""));
	hash_table_insert(d->variables,"GC_COLLECT_LIST"  , dag_variable_create(NULL, ""));
	hash_table_insert(d->variables,"MAKEFLOW_INPUTS"   , dag_variable_create(NULL, ""));
	hash_table_insert(d->variables,"MAKEFLOW_OUTPUTS"  , dag_variable_create(NULL, ""));
	*/

	/* Declare special variables */
	set_insert(d->special_vars, "CATEGORY");
	set_insert(d->special_vars, "SYMBOL");          /* Deprecated alias for CATEGORY */
	set_insert(d->special_vars, RESOURCES_CORES);
	set_insert(d->special_vars, RESOURCES_MEMORY);
	set_insert(d->special_vars, RESOURCES_DISK);
	set_insert(d->special_vars, RESOURCES_GPUS);

	/* export all variables related to resources */
	set_insert(d->export_vars, "CATEGORY");
	set_insert(d->export_vars, RESOURCES_CORES);
	set_insert(d->export_vars, RESOURCES_MEMORY);
	set_insert(d->export_vars, RESOURCES_DISK);
	set_insert(d->export_vars, RESOURCES_GPUS);

	memset(d->node_states, 0, sizeof(int) * DAG_NODE_STATE_MAX);
	return d;
}

void dag_compile_ancestors(struct dag *d)
{
	struct dag_node *n, *m;
	struct dag_file *f;
	char *name;

	hash_table_firstkey(d->files);
	while(hash_table_nextkey(d->files, &name, (void **) &f)) {
		m = f->created_by;

		if(!m)
			continue;

		list_first_item(f->needed_by);
		while((n = list_next_item(f->needed_by))) {
			debug(D_MAKEFLOW_RUN, "rule %d ancestor of %d\n", m->nodeid, n->nodeid);
			set_insert(m->descendants, n);
			set_insert(n->ancestors, m);
		}
	}
}

static int get_ancestor_depth(struct dag_node *n)
{
	int group_number = -1;
	struct dag_node *ancestor = NULL;

	debug(D_MAKEFLOW_RUN, "n->ancestor_depth: %d", n->ancestor_depth);

	if(n->ancestor_depth >= 0) {
		return n->ancestor_depth;
	}

	set_first_element(n->ancestors);
	while((ancestor = set_next_element(n->ancestors))) {

		group_number = get_ancestor_depth(ancestor);
		debug(D_MAKEFLOW_RUN, "group: %d, n->ancestor_depth: %d", group_number, n->ancestor_depth);
		if(group_number > n->ancestor_depth) {
			n->ancestor_depth = group_number;
		}
	}

	n->ancestor_depth++;
	return n->ancestor_depth;
}

void dag_find_ancestor_depth(struct dag *d)
{
	UINT64_T key;
	struct dag_node *n;

	itable_firstkey(d->node_table);
	while(itable_nextkey(d->node_table, &key, (void **) &n)) {
		get_ancestor_depth(n);
	}
}

/* Return the dag_file associated with the local name filename.
 * If one does not exist, it is created. */
struct dag_file *dag_file_lookup_or_create(struct dag *d, const char *filename)
{
	struct dag_file *f;

	f = hash_table_lookup(d->files, filename);
	if(f) return f;

	f = dag_file_create(filename);

	hash_table_insert(d->files, f->filename, (void *) f);

	return f;
}

/* Returns the struct dag_file for the local filename */
struct dag_file *dag_file_from_name(struct dag *d, const char *filename)
{
	return (struct dag_file *) hash_table_lookup(d->files, filename);
}

/* Returns the list of dag_file's which are not the target of any
 * node */
struct list *dag_input_files(struct dag *d)
{
	struct dag_file *f;
	char *filename;
	struct list *il;

	il = list_create();

	hash_table_firstkey(d->files);
	while((hash_table_nextkey(d->files, &filename, (void **) &f)))
		if(!f->created_by) {
			debug(D_MAKEFLOW_RUN, "Found independent input file: %s", f->filename);
			list_push_tail(il, f);
		}

	return il;
}


void dag_count_states(struct dag *d)
{
	struct dag_node *n;
	int i;

	for(i = 0; i < DAG_NODE_STATE_MAX; i++) {
		d->node_states[i] = 0;
	}

	for(n = d->nodes; n; n = n->next) {
		d->node_states[n->state]++;
	}
}

/**
 * If the return value is x, a positive integer, that means at least x tasks
 * can be run in parallel during a certain point of the execution of the
 * workflow. The following algorithm counts the number of direct child nodes of
 * each node (a node represents a task). Node A is a direct child of Node B
 * only when Node B is the only parent node of Node A. Then it returns the
 * maximum among the direct children counts.
 */
int dag_width_guaranteed_max(struct dag *d)
{
	struct dag_node *n, *m, *tmp;
	struct dag_file *f;
	int nodeid;
	int depends_on_single_node = 1;
	int max = 0;

	for(n = d->nodes; n; n = n->next) {
		depends_on_single_node = 1;
		nodeid = -1;
		m = 0;
		// for each source file, see if it is a target file of another node
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			// get the node (tmp) that outputs current source file
			tmp = f->created_by;
			// if a source file is also a target file
			if(tmp) {
				debug(D_MAKEFLOW_RUN, "%d depends on %d", n->nodeid, tmp->nodeid);
				if(nodeid == -1) {
					m = tmp;	// m holds the parent node
					nodeid = m->nodeid;
					continue;
				}
				// if current node depends on multiple nodes, continue to process next node
				if(nodeid != tmp->nodeid) {
					depends_on_single_node = 0;
					break;
				}
			}
		}
		// m != 0 : current node depends on at least one exsisting node
		if(m && depends_on_single_node && nodeid != -1) {
			m->only_my_children++;
		}
	}

	// find out the maximum number of direct children that a single parent node has
	for(n = d->nodes; n; n = n->next) {
		max = max < n->only_my_children ? n->only_my_children : max;
	}

	return max;
}

/**
 * returns the depth of the given DAG.
 */
int dag_depth(struct dag *d)
{
	struct dag_node *n, *parent;
	struct dag_file *f;

	struct list *level_unsolved_nodes = list_create();
	for(n = d->nodes; n != NULL; n = n->next) {
		n->level = 0;
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			if((parent = f->created_by) != NULL) {
				n->level = -1;
				list_push_tail(level_unsolved_nodes, n);
				break;
			}
		}
	}

	int max_level = 0;
	while((n = (struct dag_node *) list_pop_head(level_unsolved_nodes)) != NULL) {
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			if((parent = f->created_by) != NULL) {
				if(parent->level == -1) {
					n->level = -1;
					list_push_tail(level_unsolved_nodes, n);
					break;
				} else {
					int tmp_level = parent->level + 1;
					n->level = n->level > tmp_level ? n->level : tmp_level;
					max_level = n->level > max_level ? n->level : max_level;
				}
			}
		}
	}
	list_delete(level_unsolved_nodes);

	return max_level + 1;
}

/**
 * This algorithm assumes all the tasks take the same amount of time to execute
 * and each task would be executed as early as possible. If the return value is
 * x, a positive integer, that means at least x tasks can be run in parallel
 * during a certain point of the execution of the workflow.
 *
 * The following algorithm first determines the level (depth) of each node by
 * calling the dag_depth() function and then counts how many nodes are there at
 * each level. Then it returns the maximum of the numbers of nodes at each
 * level.
 */
int dag_width_uniform_task(struct dag *d)
{
	struct dag_node *n;

	int depth = dag_depth(d);

	size_t level_count_array_size = (depth) * sizeof(int);
	int *level_count = malloc(level_count_array_size);
	if(!level_count) {
		return -1;
	}
	memset(level_count, 0, level_count_array_size);

	for(n = d->nodes; n != NULL; n = n->next) {
		level_count[n->level]++;
	}

	int i, max = 0;
	for(i = 0; i < depth; i++) {
		if(max < level_count[i]) {
			max = level_count[i];
		}
	}

	free(level_count);
	return max;
}

/**
 * Computes the width of the graph
 */
int dag_width(struct dag *d, int nested_jobs)
{
	struct dag_node *n, *parent;
	struct dag_file *f;

	/* 1. Find the number of immediate children for all nodes; also,
	   determine leaves by adding nodes with children==0 to list. */

	for(n = d->nodes; n != NULL; n = n->next) {
		n->level = 0;	// initialize 'level' value to 0 because other functions might have modified this value.
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			parent = f->created_by;
			if(parent)
				parent->children++;
		}
	}

	struct list *leaves = list_create();

	for(n = d->nodes; n != NULL; n = n->next) {
		n->children_remaining = n->children;
		if(n->children == 0)
			list_push_tail(leaves, n);
	}

	/* 2. Assign every node a "reverse depth" level. Normally by depth,
	   I mean topologically sort and assign depth=0 to nodes with no
	   parents. However, I'm thinking I need to reverse this, with depth=0
	   corresponding to leaves. Also, we want to make sure that no node is
	   added to the queue without all its children "looking at it" first
	   (to determine its proper "depth level"). */

	int max_level = 0;

	while(list_size(leaves) > 0) {
		struct dag_node *n = (struct dag_node *) list_pop_head(leaves);

		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			parent = f->created_by;
			if(!parent)
				continue;

			if(parent->level < n->level + 1)
				parent->level = n->level + 1;

			if(parent->level > max_level)
				max_level = parent->level;

			parent->children_remaining--;
			if(parent->children_remaining == 0)
				list_push_tail(leaves, parent);
		}
	}
	list_delete(leaves);

	/* 3. Now that every node has a level, simply create an array and then
	   go through the list once more to count the number of nodes in each
	   level. */

	size_t level_count_size = (max_level + 1) * sizeof(int);
	int *level_count = malloc(level_count_size);

	memset(level_count, 0, level_count_size);

	for(n = d->nodes; n != NULL; n = n->next) {
		if(nested_jobs && !n->nested_job)
			continue;
		level_count[n->level]++;
	}

	int i, max = 0;
	for(i = 0; i <= max_level; i++) {
		if(max < level_count[i])
			max = level_count[i];
	}

	free(level_count);
	return max;
}

int dag_remote_jobs_running( struct dag *d )
{
	return itable_size(d->remote_job_table);
}

int dag_local_jobs_running( struct dag *d )
{
	return itable_size(d->local_job_table);
}

int dag_mount_clean( struct dag *d ) {
	struct list *list;
	struct dag_file *df;
	if(!d) return 0;

	list = dag_input_files(d);
	if(!list) return 0;

	list_first_item(list);
	while((df = (struct dag_file *)list_next_item(list))) {
		dag_file_mount_clean(df);
	}
	list_delete(list);

	if(d->cache_dir) {
		free(d->cache_dir);
		d->cache_dir = NULL;
	}
	return 0;
}

/* vim: set noexpandtab tabstop=4: */
