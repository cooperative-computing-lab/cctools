/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <signal.h>
#include <stdarg.h>
#include <fcntl.h>
#include <dirent.h>
#include <unistd.h>

#include "cctools.h"
#include "catalog_query.h"
#include "create_dir.h"
#include "copy_stream.h"
#include "work_queue_catalog.h"
#include "datagram.h"
#include "disk_info.h"
#include "domain_name_cache.h"
#include "link.h"
#include "macros.h"
#include "hash_table.h"
#include "itable.h"
#include "debug.h"
#include "work_queue.h"
#include "work_queue_internal.h"
#include "delete_dir.h"
#include "stringtools.h"
#include "load_average.h"
#include "get_line.h"
#include "int_sizes.h"
#include "list.h"
#include "xxmalloc.h"
#include "getopt_aux.h"
#include "rmonitor.h"
#include "random_init.h"
#include "path.h"

#include "dag.h"
#include "visitors.h"
#include "lexer.h"
#include "buffer.h"

#include "makeflow_common.h"

char *makeflow_exe = NULL;

void set_makeflow_exe(const char *makeflow_name)
{
	makeflow_exe = xxstrdup(makeflow_name);
}

const char *get_makeflow_exe()
{
	return makeflow_exe;
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
			tmp = f->target_of;
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
			if((parent = f->target_of) != NULL) {
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
			if((parent = f->target_of) != NULL) {
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
			parent = f->target_of;
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
			parent = f->target_of;
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









