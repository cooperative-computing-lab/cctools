/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "dag.h"
#include "dag_node.h"

#include "debug.h"
#include "rmsummary.h"
#include "list.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "jx.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

struct dag_node *dag_node_create(struct dag *d, int linenum)
{
	struct dag_node *n;

	n = malloc(sizeof(struct dag_node));
	memset(n, 0, sizeof(struct dag_node));
	n->d = d;
	n->linenum = linenum;
	n->state = DAG_NODE_STATE_WAITING;
	n->nodeid = d->nodeid_counter++;
	n->variables = hash_table_create(0, 0);

	n->remote_names = itable_create(0);
	n->remote_names_inv = hash_table_create(0, 0);

	n->descendants = set_create(0);
	n->ancestors = set_create(0);

	n->direct_children = set_create(0);
	n->accounted = set_create(0);

	n->source_files = list_create();
	n->source_size = 0;

	n->target_files = list_create();
	n->target_size = 0;

	n->terminal_files = set_create(0);
	n->coexist_files = set_create(0);

	n->residual_nodes = list_create();
	n->residual_files = set_create(0);
	n->residual_size = 0;

	n->run_files = set_create(0);
	n->run_footprint = 0;

	n->delete_files = set_create(0);
	n->delete_footprint = 0;
	n->delete_run_order = list_create();

	n->prog_min_files = set_create(0);
	n->prog_min_footprint = 0;

	n->prog_max_files = set_create(0);
	n->prog_max_footprint = 0;
	n->prog_run_order = list_create();

	n->footprint_min_files = set_create(0);
	n->footprint_min_size = 0;

	n->footprint_max_files = set_create(0);
	n->footprint_max_size = 0;

	n->res = 0;
	n->res_files = set_create(0);
	n->wgt = 0;
	n->wgt_files = set_create(0);
	n->max_wgt = 0;
	n->max_wgt_files = set_create(0);
	n->diff = 0;

	n->children_updated = 0;
	n->size_updated = 0;
	n->footprint_updated = 0;
	n->terminal_updated = 0;

	n->ancestor_depth = -1;

	// resources explicitely requested for only this node in the dag file.
	// PROBABLY not what you want. Most likely you want dag_node_dynamic_label(n)
	n->resources_requested = rmsummary_create(-1);

	// the value of dag_node_dynamic_label(n) when this node was submitted.
	n->resources_allocated  = rmsummary_create(-1);

	// resources used by the node, as measured by the resource_monitor (if
	// using monitoring).
	n->resources_measured  = NULL;

	n->resource_request = CATEGORY_ALLOCATION_FIRST;

	n->umbrella_spec = NULL;

	n->archive_id = NULL;

	return n;
}

int dag_node_comp(void *item, const void *arg)
{
	struct dag_node *node1 = (struct dag_node *)item;
	struct dag_node *node2 = (struct dag_node *)arg;

	if(node1 == node2)
		return 1;
	return 0;
}

int dag_node_comp_wgt(const void *item, const void *arg)
{
	struct dag_node **node1 = (void *)item;
	struct dag_node **node2 = (void *)arg;

	uint64_t size1 = (*node1)->wgt;
	uint64_t size2 = (*node2)->wgt;

	if(size1 > size2)
		return 1;
	else if(size1 < size2)
		return -1;
	return 0;
}

int dag_node_comp_wgt_rev(const void *item, const void *arg)
{
	struct dag_node **node1 = (void *)item;
	struct dag_node **node2 = (void *)arg;

	uint64_t size1 = (*node1)->wgt;
	uint64_t size2 = (*node2)->wgt;

	if(size1 < size2)
		return 1;
	else if(size1 > size2)
		return -1;
	return 0;
}

int dag_node_comp_res(const void *item, const void *arg)
{
	struct dag_node **node1 = (void *)item;
	struct dag_node **node2 = (void *)arg;

	uint64_t size1 = (*node1)->res;
	uint64_t size2 = (*node2)->res;

	if(size1 > size2)
		return 1;
	else if(size1 < size2)
		return -1;
	return 0;
}

int dag_node_comp_diff(const void *item, const void *arg)
{
	struct dag_node **node1 = (void *)item;
	struct dag_node **node2 = (void *)arg;

	uint64_t size1 = (*node1)->diff;
	uint64_t size2 = (*node2)->diff;

	if(size1 > size2)
		return -1;
	else if(size1 < size2)
		return 1;

	size1 = (*node1)->res;
	size2 = (*node2)->res;

	if(size1 > size2)
		return -1;
	else if(size1 < size2)
		return 1;

	return 0;
}

const char *dag_node_state_name(dag_node_state_t state)
{
	switch (state) {
	case DAG_NODE_STATE_WAITING:
		return "waiting";
	case DAG_NODE_STATE_RUNNING:
		return "running";
	case DAG_NODE_STATE_COMPLETE:
		return "complete";
	case DAG_NODE_STATE_FAILED:
		return "failed";
	case DAG_NODE_STATE_ABORTED:
		return "aborted";
	default:
		return "unknown";
	}
}

/* Returns the remotename used in rule n for local name filename */
const char *dag_node_get_remote_name(struct dag_node *n, const char *filename)
{
	struct dag_file *f;
	char *name;

	f = dag_file_from_name(n->d, filename);
	name = (char *) itable_lookup(n->remote_names, (uintptr_t) f);

	return name;
}

/* Returns the local name of filename */
const char *dag_node_get_local_name(struct dag_node *n, const char *filename)
{
	struct dag_file *f;
	const char *name;

	f = hash_table_lookup(n->remote_names_inv, filename);

	if(!f)
	{
		name =  NULL;
	}
	else
	{
		name = f->filename;
	}

	return name;
}

void dag_node_set_umbrella_spec(struct dag_node *n, const char *umbrella_spec)
{
	struct stat st;

	if(!n) return;

	if(lstat(umbrella_spec, &st) == -1) {
		fatal("lstat(`%s`) failed: %s\n", umbrella_spec, strerror(errno));
	}
	if((st.st_mode & S_IFMT) != S_IFREG) {
		fatal("the umbrella spec (`%s`) should specify a regular file\n", umbrella_spec);
	}

	n->umbrella_spec = umbrella_spec;
}

/* Translate an absolute filename into a unique slash-less name to allow for the
   sending of any file to remote systems. The function allows for upto a million name collisions. */
static char *dag_node_translate_filename(struct dag_node *n, const char *filename)
{
	int len;
	char *newname_ptr;

	len = strlen(filename);

	/* If there are no slashes in path, then we don't need to translate. */
	if(!strchr(filename, '/')) {
		newname_ptr = xxstrdup(filename);
		return newname_ptr;
	}

	/* If the filename is in the current directory and doesn't contain any
	 * additional slashes, then we can also skip translation.
	 *
	 * Note: this doesn't handle redundant ./'s such as ./././././foo/bar */
	if(!strncmp(filename, "./", 2) && !strchr(filename + 2, '/')) {
		newname_ptr = xxstrdup(filename);
		return newname_ptr;
	}

	/* Make space for the new filename + a hyphen + a number to
	 * handle upto a million name collisions */
	newname_ptr = calloc(len + 8, sizeof(char));
	strcpy(newname_ptr, filename);

	char *c;
	for(c = newname_ptr; *c; ++c) {
		switch (*c) {
		case '/':
		case '.':
			*c = '_';
			break;
		default:
			break;
		}
	}

	if(!n)
		return newname_ptr;

	int i = 0;
	char *newname_org = xxstrdup(newname_ptr);
	while(hash_table_lookup(n->remote_names_inv, newname_ptr)) {
		sprintf(newname_ptr, "%06d-%s", i, newname_org);
		i++;
	}

	free(newname_org);

	return newname_ptr;
}

/* Adds remotename to the local name filename in the namespace of
 * the given node. If remotename is NULL, then a new name is
 * found using dag_node_translate_filename. If the remotename
 * given is different from a previosly specified, a warning is
 * written to the debug output, but otherwise this is ignored. */
static const char *dag_node_add_remote_name(struct dag_node *n, const char *filename, const char *remotename)
{
	char *oldname;
	struct dag_file *f = dag_file_from_name(n->d, filename);

	if(!f)
		fatal("trying to add remote name %s to unknown file %s.\n", remotename, filename);

	if(!remotename)
		remotename = dag_node_translate_filename(n, filename);
	else
		remotename = xxstrdup(remotename);

	oldname = hash_table_lookup(n->remote_names_inv, remotename);

	if(oldname && strcmp(oldname, filename) == 0)
		debug(D_MAKEFLOW_RUN, "Remote name %s for %s already in use for %s\n", remotename, filename, oldname);

	itable_insert(n->remote_names, (uintptr_t) f, remotename);
	hash_table_insert(n->remote_names_inv, remotename, (void *) f);

	return remotename;
}

/* Adds the local name to the list of source files of the node,
 * and adds the node as a dependant of the file. If remotename is
 * not NULL, it is added to the namespace of the node. */
void dag_node_add_source_file(struct dag_node *n, const char *filename, const char *remotename)
{
	struct dag_file *source = dag_file_lookup_or_create(n->d, filename);

	if(remotename)
		dag_node_add_remote_name(n, filename, remotename);

	/* register this file as a source of the node */
	list_push_head(n->source_files, source);

	/* register this file as a requirement of the node */
	list_push_head(source->needed_by, n);

	source->reference_count++;
}

/* Adds the local name as a target of the node, and register the
 * node as the producer of the file. If remotename is not NULL,
 * it is added to the namespace of the node. */
void dag_node_add_target_file(struct dag_node *n, const char *filename, char *remotename)
{
	struct dag_file *target = dag_file_lookup_or_create(n->d, filename);

	if(target->created_by && target->created_by != n)
		fatal("%s is defined multiple times at %s:%d and %s:%d\n", filename, filename, target->created_by->linenum, filename, n->linenum);

	if(remotename)
		dag_node_add_remote_name(n, filename, remotename);

	/* register this file as a target of the node */
	list_push_head(n->target_files, target);

	/* register this node as the creator of the file */
	target->created_by = n;
}

/* Function that calculates the three different footprint values and
 * stores the largest as the key footprint of the node. */
void dag_node_determine_children(struct dag_node *n)
{
	struct dag_node *c;

	/* Have un-updated children calculate their current footprint. */
	set_first_element(n->descendants);
	while((c = set_next_element(n->descendants))){
		if(!c->children_updated){
			dag_node_determine_children(c);
		}
		set_insert_set(n->accounted, c->accounted);
	}

	set_first_element(n->descendants);
	while((c = set_next_element(n->descendants))){
		if(!set_lookup(n->accounted, c)){
			set_insert(n->direct_children, c);
			set_insert(n->accounted, c);
		}
	}

	n->children_updated = 1;
}

uint64_t dag_node_file_list_size(struct list *s)
{
	struct dag_file *f;
	uint64_t size = 0;
	list_first_item(s);
	while((f = list_next_item(s)))
		size += dag_file_size(f);

	return size;
}

uint64_t dag_node_file_set_size(struct set *s)
{
	struct dag_file *f;
	uint64_t size = 0;
	set_first_element(s);
	while((f = set_next_element(s)))
		size += dag_file_size(f);

	return size;
}

int dag_node_file_coexist_files(struct set *s, struct dag_file *f)
{
	struct dag_node *n;
	list_first_item(f->needed_by);
	while((n = list_next_item(f->needed_by))){
		if(set_lookup(s, n))
			return 1;
	}
	return 0;
}

void dag_node_prepare_node_terminal_files(struct dag_node *n)
{
	struct dag_file *f;
	list_first_item(n->target_files);
	while((f = list_next_item(n->target_files))){
		if(f->type == DAG_FILE_TYPE_OUTPUT){
			set_push(n->terminal_files, f);
		}
		set_push(n->coexist_files, f);
	}

	struct dag_node *node1;
	set_first_element(n->ancestors);
	while((node1 = set_next_element(n->ancestors))){
		set_insert_set(n->terminal_files, node1->terminal_files);

		set_first_element(node1->coexist_files);
		while((f = set_next_element(node1->coexist_files))){
			if(dag_node_file_coexist_files(n->accounted, f))
				set_push(n->coexist_files, f);
		}
	}

	set_first_element(n->descendants);
	while((node1 = set_next_element(n->descendants))){
		node1->terminal_updated--;
		if(node1->terminal_updated <= 0)
			dag_node_prepare_node_terminal_files(node1);
	}
}

void dag_node_prepare_node_size(struct dag_node *n)
{
	struct dag_node *s;

	/* Determine source size based on either the actual inputs or the
		estimated size of the inputs and store in source_size */
	n->source_size = dag_node_file_list_size(n->source_files);

	/* Determine target size based on either the actual outputs or the
		estimated size of the outputs and store in target_size */
	n->target_size = dag_node_file_list_size(n->target_files);

	/* Recursively updated children if they have not yet been updated */
	set_first_element(n->direct_children);
	while((s = set_next_element(n->direct_children)) && !s->size_updated)
		dag_node_prepare_node_size(s);

	/* Mark this node as having been updated for size */
	n->size_updated = 1;
}

/* The run footprint of a node is defined as its target size and
 * the size of it inputs. It is the cost needed to run this node. */
void dag_node_determine_run_footprint(struct dag_node *n)
{
	set_delete(n->run_files);
	n->run_files = set_create(0);
	set_insert_list(n->run_files, n->source_files);
	set_insert_list(n->run_files, n->target_files);
	set_insert_set(n->run_files, n->terminal_files);
	set_insert_set(n->run_files, n->coexist_files);

	n->run_footprint = dag_node_file_set_size(n->run_files);
}

/* The descendant footprint of a node is defined as a balance between
 * the widest point of the children branches, while still maintaining
 * the existance of the sibling branches. The assumption is that by
 * knowing the larget size needed, all other branches can be executed
 * within that designated size, so we only need to add the residual
 * size of a branch to hold onto it while the heavier weights are
 * computed. */
void dag_node_determine_descendant_footprint(struct dag_node *n)
{
	struct dag_node *node1, *node2; //, *res_node;
	struct list *tmp_direct_children = list_create();
	struct set *footprint = set_create(0);
	uint64_t footprint_size = 0;

	/* Create a second list of descendants that allows us to
		iterate through while holding out current. This is needed
		as we compare footprint and the residual nodes.
		We also set the residual and footprint list to first. */
	set_first_element(n->direct_children);
	while((node1 = set_next_element(n->direct_children))){
		list_push_tail(tmp_direct_children, node1);
	}

	set_first_element(n->descendants);
	while((node1 = set_next_element(n->descendants))){
		list_first_item(node1->residual_nodes);
	}


	/* Clear existing list to prevent complicated modifications. */
	list_delete(n->residual_nodes);
	list_delete(n->run_order);

	/* There are three cases for descendant nodes:
		1. Multiple direct_children indicating that multiple branches will
			need to be maintained concurrently and we need to account.
		2. Only one descendant indicating we want to continue the chain
			of residual and footprints that out child holds.
		3. No children indication we are the start of a branch and need
			create empty lists for this case.
	*/
	set_first_element(n->direct_children);
	if(set_size(n->direct_children) > 1){
		/* Case 1. We need to find common sublists and determine the
			weight. */
		n->residual_nodes = list_create();
		n->run_order = list_create();

		/* This finds the intersect of all of the children lists. This
			intersect forms the basis for the parents residual nodes as
			all sub-branches will culminate in the listed nodes. */
		int comp = 1;
		int index = 0;
		while(comp){
			index++;
			node1 = set_next_element(n->direct_children); // Get first child
			node1 = list_peek_current(node1->residual_nodes); // Grab next node in its list
			while((node2 = set_next_element(n->direct_children))){ // Loop over remaining children
				node2 = list_peek_current(node2->residual_nodes);
				/* We mark when the nodes are no longer comparable, but do
					not break as we need all of the lists to be in the first
					non-shared location for future use. */
				if(!node1 || !node2 || (node1 != node2))
					comp = 0;
			}

			set_first_element(n->direct_children);
			/* Only add the node if it occurred in all of the branch lists. */
			if(comp){
				list_push_tail(n->residual_nodes, node1);
				//res_node = node1;
				/* Advance all direct_children forward one residual. */
				while((node1 = set_next_element(n->direct_children))){
					list_next_item(node1->residual_nodes);
				}
				set_first_element(n->direct_children);
			}
		}

		set_first_element(n->direct_children);
		while((node1 = set_next_element(n->direct_children))){
			node2 = list_peek_current(node1->residual_nodes);

			/* Add the last residual's residual and terminal files in the branch
				to the current residual files */
			set_insert_set(n->residual_files, node2->residual_files);
			set_insert_set(n->residual_files, node2->terminal_files);

			/* Add the last residual's residual and terminal files in the branch
				to the branch's first node residual files */
			set_insert_set(node1->res_files, node2->residual_files);
			set_insert_set(node1->res_files, node2->terminal_files);

			/* Set branch head's res size */
			node1->res = dag_node_file_set_size(node1->res_files);

			set_insert_set(node1->wgt_files, node2->footprint_min_files);
			node1->wgt = node2->footprint_min_size;

			set_insert_set(node1->max_wgt_files, node2->footprint_max_files);
			node1->max_wgt = node2->footprint_max_size;

			list_next_item(node1->residual_nodes);
			while((node2 = list_peek_current(node1->residual_nodes))){
				if(node2->footprint_min_size >= node1->wgt){
					set_delete(node1->wgt_files);
					node1->wgt_files = set_duplicate(node2->footprint_min_files);
					node1->wgt = node2->footprint_min_size;
				}
				if(node2->footprint_max_size >= node1->max_wgt){
					set_delete(node1->max_wgt_files);
					node1->max_wgt_files = set_duplicate(node2->footprint_max_files);
					node1->max_wgt = node2->footprint_max_size;
				}
				list_next_item(node1->residual_nodes);
			}
		}

		set_first_element(n->direct_children);
		while((node1 = set_next_element(n->direct_children))){
			node1->diff = node1->wgt - node1->res;
		}

		n->residual_size = dag_node_file_set_size(n->residual_files);

		/* Clear any previously defined max though it should not exist. */
		set_delete(n->prog_max_files);
		n->prog_max_files = set_create(0);

		set_insert_list(footprint, n->target_files);

		list_sort(tmp_direct_children, dag_node_comp_diff);
		list_first_item(tmp_direct_children);
		/* Loop over each child giving it the chance to be the largest footprint. */
		while((node1 = list_next_item(tmp_direct_children))){
			footprint_size = dag_node_file_set_size(footprint);
			if((footprint_size + node1->wgt) > n->delete_footprint){
				set_delete(n->delete_files);
				n->delete_files = set_duplicate(footprint);
				set_insert_set(n->delete_files, node1->wgt_files);
				n->delete_footprint = dag_node_file_set_size(n->delete_files);

			}
			// This is where we would remove an input file is it wasn't needed for other branches
			set_insert_set(footprint, node1->res_files);
			list_push_tail(n->delete_run_order, node1);
		}

		list_sort(tmp_direct_children, dag_node_comp_wgt_rev);
		list_first_item(tmp_direct_children);
		node1 = list_next_item(tmp_direct_children);

		set_insert_set(n->prog_max_files, node1->max_wgt_files);
		set_insert_set(n->prog_min_files, node1->wgt_files);
		list_push_tail(n->prog_run_order, node1);

		/* Find what the total space is needed to hold all residuals and
			the largest footprint branch concurrently. */
		while((node2 = list_next_item(tmp_direct_children))){
			set_insert_set(n->prog_max_files, node2->max_wgt_files);
			set_insert_set(n->prog_min_files, node2->res_files);
			list_push_tail(n->prog_run_order, node2);
		}

		n->prog_max_footprint = dag_node_file_set_size(n->prog_max_files);
		n->prog_min_footprint = dag_node_file_set_size(n->prog_min_files);
	} else if(set_size(n->direct_children) == 1){
		/* Case 2. Extend the lists maintained at the child with
			the exception of run order which is just the child. */
		node1 = set_next_element(n->direct_children);
		n->run_order = list_create();
		list_push_tail(n->run_order, node1);

		n->residual_nodes = list_duplicate(node1->residual_nodes);
		set_insert_list(n->residual_files, n->target_files);
		set_insert_set(n->residual_files, n->terminal_files);
		n->residual_size = dag_node_file_set_size(n->residual_files);
	} else {
		/* Case 3. Create empty lists reflecting lack of children. */
		n->residual_nodes = list_create();
		set_insert_list(n->residual_files, n->target_files);
		set_insert_set(n->residual_files, n->terminal_files);
		n->residual_size = n->target_size;
		n->run_order = list_create();
	}

	/* Adding the current nodes list so parents can quickly access
		these decisions. */
	list_push_tail(n->residual_nodes, n);

	list_delete(tmp_direct_children);
}

/* Function that allows the purpose to be succintly stated. We only
 * store the max as the true weight of the node, so this allows it to
 * be clearly expressed. */
void dag_node_min_footprint( struct dag_node *n)
{
	set_delete(n->footprint_min_files);
	if(n->delete_footprint <= n->prog_min_footprint){
		n->footprint_min_size = n->delete_footprint;
		n->footprint_min_files = set_duplicate(n->delete_files);
		n->footprint_min_type = DAG_NODE_FOOTPRINT_DELETE;
		n->run_order = n->delete_run_order;
	} else {
		n->footprint_min_size = n->prog_min_footprint;
		n->footprint_min_files = set_duplicate(n->prog_min_files);
		n->footprint_min_type = DAG_NODE_FOOTPRINT_DESC;
		n->run_order = n->prog_run_order;
	}

	n->self_res = n->target_size;
	if(n->self_res < n->footprint_min_size){
		n->self_res = n->footprint_min_size;
	}

	if(n->run_footprint > n->footprint_min_size){
		set_delete(n->footprint_min_files);
		n->footprint_min_size = n->run_footprint;
		n->footprint_min_files = set_duplicate(n->run_files);
		n->footprint_min_type = DAG_NODE_FOOTPRINT_RUN;
	}
}

/* Function that allows the purpose to be succintly stated. We only
 * store the max as the true weight of the node, so this allows it to
 * be clearly expressed. */
void dag_node_max_footprint( struct dag_node *n)
{
	if(n->prog_max_footprint > n->footprint_max_size){
		set_delete(n->footprint_max_files);
		n->footprint_max_size = n->prog_max_footprint;
		n->footprint_max_files = set_duplicate(n->prog_max_files);
	}

	if(n->delete_footprint > n->footprint_max_size){
		set_delete(n->footprint_max_files);
		n->footprint_max_size = n->delete_footprint;
		n->footprint_max_files = set_duplicate(n->delete_files);
	}

	if(n->run_footprint > n->footprint_max_size){
		set_delete(n->footprint_max_files);
		n->footprint_max_size = n->run_footprint;
		n->footprint_max_files = set_duplicate(n->run_files);
	}
}

void dag_node_set_run_order_dependencies(struct dag_node *n)
{
	struct dag_node *node1 = n;
	struct dag_node *node2;
	list_first_item(n->run_order);
	while((node2 = list_next_item(n->run_order))){
		if(node2->dependencies)
			set_delete(node2->dependencies);

		node2->dependencies = set_create(0);
		set_insert(node2->dependencies, node1);
		node1 = node2;
	}
}

/* Function that calculates the three different footprint values and
 * stores the largest as the key footprint of the node. */
void dag_node_determine_footprint(struct dag_node *n)
{
	struct dag_node *c;

	dag_node_determine_run_footprint(n);

	/* Have un-updated children calculate their current footprint. */
	set_first_element(n->direct_children);
	while((c = set_next_element(n->direct_children))){
		if(!c->footprint_updated)
			dag_node_determine_footprint(c);
	}

	dag_node_determine_descendant_footprint(n);

	/* Finds the max of all three different weights. */
	dag_node_min_footprint(n);

	/* Finds the max of all three different weights. */
	dag_node_max_footprint(n);

	/* Mark node as having been updated. */
	n->footprint_updated = 1;
}

/* qsort C-string comparison function */
int cstring_cmp(const void *a, const void *b)
{
	const char **ia = (const char **)a;
	const char **ib = (const char **)b;
	return strcmp(*ia, *ib);
	/* strcmp functions works exactly as expected from
 *	comparison function */
}

void dag_node_print_node_set(struct set *s, FILE *out, char *t)
{
	if(!s){
		fprintf(out, "\\{\\}%s", t);
		return;
	}

	set_first_element(s);
	struct dag_node *n;
	if(set_size(s) == 0){
		fprintf(out, "\\{\\}%s", t);
	} else {
		n = set_next_element(s);
		fprintf(out, "\\{%d", n->nodeid);
		while((n = set_next_element(s))){
			fprintf(out, ",%d", n->nodeid);
		}
		fprintf(out, "\\}%s", t);
	}
}

void dag_node_print_node_list(struct list *s, FILE *out, char *t)
{
	if(!s){
		fprintf(out, "\\{\\}%s", t);
		return;
	}

	list_first_item(s);
	struct dag_node *n;
	if(list_size(s) == 0){
		fprintf(out, "\\{\\}%s", t);
	} else {
		n = list_next_item(s);
		fprintf(out, "\\{%d", n->nodeid);
		while((n = list_next_item(s))){
			fprintf(out, ",%d", n->nodeid);
		}
		fprintf(out, "\\}%s", t);
	}
}

void dag_node_print_file_set(struct set *s, FILE *out, char *t)
{
	if(!s){
		fprintf(out, "\\{\\}%s", t);
		return;
	}

	set_first_element(s);
	struct dag_file *f;
	if(set_size(s) == 0){
		fprintf(out, "\\{\\}%s", t);
	} else {
		fprintf(out, "\\{");
		const char *files[set_size(s)];
		int index = 0;
		while((f = set_next_element(s))){
			files[index] = f->filename;
			index++;
		}

		qsort(files, index, sizeof(char *), cstring_cmp);
		for(int i = 0; i < index; i++){
			fprintf(out, "%s", files[i]);
		}

		fprintf(out, "\\}%s", t);
	}
}

void dag_node_print_footprint_node(struct dag_node *n, FILE *out, char *retrn, char *node_retrn, char *delim)
{
	fprintf(out, "%d%s", n->nodeid, delim);

	int numeric = 1;
	int symbolic = 1;

	if(numeric){
		fprintf(out, "%"PRIu64"%s", n->footprint_min_size, delim);
		fprintf(out, "%"PRIu64"%s", n->footprint_max_size, delim);
		fprintf(out, "%"PRIu64"%s", n->residual_size, delim);
		fprintf(out, "%"PRIu64"%s", n->run_footprint, delim);
		fprintf(out, "%"PRIu64"%s", n->delete_footprint, delim);
		fprintf(out, "%"PRIu64"%s", n->prog_min_footprint, delim);
		fprintf(out, "%"PRIu64"%s", n->prog_max_footprint, node_retrn);
		if(symbolic) {
			dag_node_print_node_list(n->residual_nodes, out, delim);
		} else {
			dag_node_print_node_list(n->residual_nodes, out, delim);
			fprintf(out,"%s%s%s%s%s%s%s",
				delim,delim,delim,delim,delim,delim,retrn);
		}
	}

	if(symbolic){
		dag_node_print_file_set(n->footprint_min_files, out, delim);
		dag_node_print_file_set(n->footprint_max_files, out, delim);
		dag_node_print_file_set(n->residual_files, out, delim);
		dag_node_print_file_set(n->run_files, out, delim);
		dag_node_print_file_set(n->delete_files, out, delim);
		dag_node_print_file_set(n->prog_min_files, out, delim);
		dag_node_print_file_set(n->prog_max_files, out, retrn);
	}
}

void dag_node_print_footprint(struct dag *d, char *output)
{
	struct dag_node *n;

	int tex = 1;

	char *retrn = "\n";
	char *node_retrn = "\n";
	char *delim = "\t";

	if(tex){
		retrn = "\\\\ \\hline \n\t";
		node_retrn = "\\\\ \n\t";
		delim = " & ";
	}

	FILE * out;
	out = fopen(output, "w");

	if(tex)
		fprintf(out, "\\begin{tabular}{|cccccccc|}\n\t\\hline\n");

	fprintf(out, "Node%s",delim);
	fprintf(out, "Foot-Min%s",delim);
	fprintf(out, "Foot-Max%s",delim);
	fprintf(out, "Residual%s",delim);
	fprintf(out, "Parent%s",delim);
	fprintf(out, "Child%s",delim);
	fprintf(out, "Desc-Min%s",delim);
	fprintf(out, "Desc-Max%s",node_retrn);
	fprintf(out, "Res Nodes%s%s%s%s%s%s%s%s",
				delim,delim,delim,delim,delim,delim,delim,retrn);

	for(n = d->nodes; n; n = n->next) {
		dag_node_print_footprint_node(n, out, retrn, node_retrn, delim);
	}

	if(tex)
		fprintf(out, "\\end{tabular}\n");

	fclose(out);
}

/* After a node has been completed mark that it and its
 * children are in need of being updated. */
void dag_node_reset_updated(struct dag_node *n)
{
	struct dag_node *n1;
	set_first_element(n->direct_children);
	while((n1 = set_next_element(n->direct_children))){
		if(n1->footprint_updated || !n->terminal_updated)
			dag_node_reset_updated(n1);
	}
	n->size_updated = 0;
	n->footprint_updated = 0;
	n->terminal_updated = set_size(n->ancestors);
}

void dag_node_init_resources(struct dag_node *n)
{
	struct rmsummary *rs    = n->resources_requested;
	struct dag_variable_lookup_set s_node = { NULL, NULL, n, NULL };
	struct dag_variable_lookup_set s_all  = { n->d, n->category, n, NULL };

	struct dag_variable_value *val;

	/* first pass, only node variables. We only check if this node was individually labeled. */
	val = dag_variable_lookup(RESOURCES_CORES, &s_node);
	if(val)
		n->resource_request = CATEGORY_ALLOCATION_USER;

	val = dag_variable_lookup(RESOURCES_DISK, &s_node);
	if(val)
		n->resource_request = CATEGORY_ALLOCATION_USER;

	val = dag_variable_lookup(RESOURCES_MEMORY, &s_node);
	if(val)
		n->resource_request = CATEGORY_ALLOCATION_USER;

	val = dag_variable_lookup(RESOURCES_GPUS, &s_node);
	if(val)
		n->resource_request = CATEGORY_ALLOCATION_USER;

	int category_flag = 0;
	/* second pass: fill fall-back values if at least one resource was individually labeled. */
	/* if not, resources will come from the category when submitting. */
	val = dag_variable_lookup(RESOURCES_CORES, &s_all);
	if(val) {
		category_flag = 1;
		rs->cores = atoll(val->value);
	}

	val = dag_variable_lookup(RESOURCES_DISK, &s_all);
	if(val) {
		category_flag = 1;
		rs->disk = atoll(val->value);
	}

	val = dag_variable_lookup(RESOURCES_MEMORY, &s_all);
	if(val) {
		category_flag = 1;
		rs->memory = atoll(val->value);
	}

	val = dag_variable_lookup(RESOURCES_GPUS, &s_all);
	if(val) {
		category_flag = 1;
		rs->gpus = atoll(val->value);
	}

	if(n->resource_request != CATEGORY_ALLOCATION_USER && category_flag) {
		n->resource_request = CATEGORY_ALLOCATION_AUTO_ZERO;
	}
}

void dag_node_print_debug_resources(struct dag_node *n)
{
	const struct rmsummary *r = dag_node_dynamic_label(n);

	if(!r)
		return;

	if( r->cores > -1 )
		debug(D_MAKEFLOW_RUN, "cores:  %"PRId64".\n",      r->cores);
	if( r->memory > -1 )
		debug(D_MAKEFLOW_RUN, "memory:   %"PRId64" MB.\n", r->memory);
	if( r->disk > -1 )
		debug(D_MAKEFLOW_RUN, "disk:     %"PRId64" MB.\n", r->disk);
	if( r->gpus > -1 )
		debug(D_MAKEFLOW_RUN, "gpus:  %"PRId64".\n",       r->gpus);
}

/*
Creates a jx object containing the explicit environment
strings for this given node.
*/

struct jx * dag_node_env_create( struct dag *d, struct dag_node *n )
{
	struct dag_variable_lookup_set s = { d, n->category, n, NULL };
	char *key;

	struct jx *object = jx_object(0);

	char *num_cores = dag_variable_lookup_string(RESOURCES_CORES, &s);
	char *num_omp_threads = dag_variable_lookup_string("OMP_NUM_THREADS", &s);

	if (num_cores && !num_omp_threads) {
		// if number of cores is set, number of omp threads is not set,
		// then we set number of omp threads to number of cores
		jx_insert(object, jx_string("OMP_NUM_THREADS"), jx_string(num_cores));
	} else if (num_omp_threads) {
		// if number of omp threads is set, then we set number of cores
		// to the number of omp threads
		jx_insert(object, jx_string(RESOURCES_CORES), jx_string(num_omp_threads));
	} else {
		// if both number of cores and omp threads are not set, we
		// set them to 1
		jx_insert(object, jx_string("OMP_NUM_THREADS"), jx_string("1"));
		jx_insert(object, jx_string(RESOURCES_CORES), jx_string("1"));
	}

	set_first_element(d->export_vars);
	while((key = set_next_element(d->export_vars))) {
		char *value = dag_variable_lookup_string(key, &s);
		if(value) {
			jx_insert(object,jx_string(key),jx_string(value));
			debug(D_MAKEFLOW_RUN, "export %s=%s", key, value);
		}
	}

	free(num_cores);
	free(num_omp_threads);

	return object;
}

/* Return resources according to request. */

const struct rmsummary *dag_node_dynamic_label(const struct dag_node *n) {
	return category_dynamic_task_max_resources(n->category, NULL, n->resource_request);
}

/* vim: set noexpandtab tabstop=4: */
