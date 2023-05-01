/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "dag.h"
#include "dag_node_footprint.h"
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

struct dag_node_footprint *dag_node_footprint_create()
{
	struct dag_node_footprint *f;

	f = malloc(sizeof(struct dag_node_footprint));

	f->direct_children = set_create(0);
	f->accounted = set_create(0);

	f->source_size = 0;
	f->target_size = 0;

	f->terminal_files = set_create(0);
	f->coexist_files = set_create(0);

	f->residual_nodes = list_create();
	f->residual_files = set_create(0);
	f->residual_size = 0;

	f->run_files = set_create(0);
	f->run_footprint = 0;

	f->delete_files = set_create(0);
	f->delete_footprint = 0;
	f->delete_run_order = list_create();

	f->prog_min_files = set_create(0);
	f->prog_min_footprint = 0;

	f->prog_max_files = set_create(0);
	f->prog_max_footprint = 0;
	f->prog_run_order = list_create();

	f->footprint_min_files = set_create(0);
	f->footprint_min_size = 0;

	f->footprint_max_files = set_create(0);
	f->footprint_max_size = 0;

	f->res = 0;
	f->res_files = set_create(0);
	f->wgt = 0;
	f->wgt_files = set_create(0);
	f->max_wgt = 0;
	f->max_wgt_files = set_create(0);
	f->diff = 0;

	f->children_updated = 0;
	f->size_updated = 0;
	f->footprint_updated = 0;
	f->terminal_updated = 0;

	return f;
}

void dag_node_footprint_delete(struct dag_node_footprint *f)
{
	set_delete(f->direct_children);
	set_delete(f->accounted);

	set_delete(f->terminal_files);
	set_delete(f->coexist_files);

	list_delete(f->residual_nodes);
	set_delete(f->residual_files);

	set_delete(f->run_files);

	set_delete(f->delete_files);
	list_delete(f->delete_run_order);

	set_delete(f->prog_min_files);

	set_delete(f->prog_max_files);
	list_delete(f->prog_run_order);

	set_delete(f->footprint_min_files);
	set_delete(f->footprint_max_files);

	set_delete(f->res_files);
	set_delete(f->wgt_files);
	set_delete(f->max_wgt_files);

	free(f);
}

int dag_node_footprint_comp_wgt_rev(const void *item, const void *arg)
{
	struct dag_node **node1 = (void *)item;
	struct dag_node **node2 = (void *)arg;

	if((*node1)->footprint->wgt > (*node2)->footprint->wgt)
		return 1;
	else if((*node1)->footprint->wgt < (*node2)->footprint->wgt)
		return -1;

	return 0;
}

int dag_node_footprint_comp_diff(const void *item, const void *arg)
{
	struct dag_node **node1 = (void *)item;
	struct dag_node **node2 = (void *)arg;
	
	if((*node2)->footprint->diff > (*node1)->footprint->diff)
		return 1;
	else if((*node2)->footprint->diff < (*node1)->footprint->diff)
		return -1;

	if((*node2)->footprint->res < (*node1)->footprint->res)
		return 1;
	else if((*node2)->footprint->res > (*node1)->footprint->res)
		return -1;

	return 0;
}


/* Function that calculates the three different footprint values and
 * stores the largest as the key footprint of the node. */
void dag_node_footprint_determine_children(struct dag_node *n)
{
	struct dag_node *c;

	if(!n->footprint)
		n->footprint = dag_node_footprint_create();

	/* Have un-updated children calculate their direct children. */
	set_first_element(n->descendants);
	while((c = set_next_element(n->descendants))){
		if(!(c->footprint && c->footprint->children_updated)){
			dag_node_footprint_determine_children(c);
		}
		set_insert_set(n->footprint->accounted, c->footprint->accounted);
	}

	set_first_element(n->descendants);
	while((c = set_next_element(n->descendants))){
		if(!set_lookup(n->footprint->accounted, c)){
			set_insert(n->footprint->direct_children, c);
			set_insert(n->footprint->accounted, c);
		}
	}

	n->footprint->children_updated = 1;
}

void dag_node_footprint_prepare_node_terminal_files(struct dag_node *n)
{
	struct dag_file *f;
	list_first_item(n->target_files);
	while((f = list_next_item(n->target_files))){
		if(f->type == DAG_FILE_TYPE_OUTPUT){
			set_push(n->footprint->terminal_files, f);
		}
		set_push(n->footprint->coexist_files, f);
	}

	struct dag_node *node1;
	set_first_element(n->ancestors);
	while((node1 = set_next_element(n->ancestors))){
		set_insert_set(n->footprint->terminal_files, node1->footprint->terminal_files);

		set_first_element(node1->footprint->coexist_files);
		while((f = set_next_element(node1->footprint->coexist_files))){
			if(dag_file_coexist_files(n->footprint->accounted, f))
				set_push(n->footprint->coexist_files, f);
		}
	}

	set_first_element(n->descendants);
	while((node1 = set_next_element(n->descendants))){
		node1->footprint->terminal_updated--;
		if(node1->footprint->terminal_updated <= 0)
			dag_node_footprint_prepare_node_terminal_files(node1);
	}
}

void dag_node_footprint_prepare_node_size(struct dag_node *n)
{
	struct dag_node *s;

	/* Determine source size based on either the actual inputs or the
		estimated size of the inputs and store in source_size */
	n->footprint->source_size = dag_file_list_size(n->source_files);

	/* Determine target size based on either the actual outputs or the
		estimated size of the outputs and store in target_size */
	n->footprint->target_size = dag_file_list_size(n->target_files);

	/* Recursively updated children if they have not yet been updated */
	set_first_element(n->footprint->direct_children);
	while((s = set_next_element(n->footprint->direct_children)) && !s->footprint->size_updated)
		dag_node_footprint_prepare_node_size(s);

	/* Mark this node as having been updated for size */
	n->footprint->size_updated = 1;
}

/* The run footprint of a node is defined as its target size and
 * the size of it inputs. It is the cost needed to run this node. */
void dag_node_footprint_determine_run_footprint(struct dag_node *n)
{
	set_delete(n->footprint->run_files);
	n->footprint->run_files = set_create(0);
	set_insert_list(n->footprint->run_files, n->source_files);
	set_insert_list(n->footprint->run_files, n->target_files);
	set_insert_set(n->footprint->run_files, n->footprint->terminal_files);
	set_insert_set(n->footprint->run_files, n->footprint->coexist_files);

	n->footprint->run_footprint = dag_file_set_size(n->footprint->run_files);
}

/* This finds the intersect of all of the children lists. This
	intersect forms the basis for the parents residual nodes as
	all sub-branches will culminate in the listed nodes. */
void dag_node_footprint_determine_desc_residual_intersect(struct dag_node *n)
{
	struct dag_node *node1, *node2;

	int comp = 1;
	while(comp){
		node1 = set_next_element(n->footprint->direct_children); // Get first child
		node1 = list_peek_current(node1->footprint->residual_nodes); // Grab next node in its list
		while((node2 = set_next_element(n->footprint->direct_children))){ // Loop over remaining children
			node2 = list_peek_current(node2->footprint->residual_nodes);
			/* We mark when the nodes are no longer comparable, but do
				not break as we need all of the lists to be in the first
				non-shared location for future use. */
			if(!node1 || !node2 || (node1 != node2))
				comp = 0;
		}

		set_first_element(n->footprint->direct_children);
		/* Only add the node if it occurred in all of the branch lists. */
		if(comp){
			list_push_tail(n->footprint->residual_nodes, node1);
			//res_node = node1;
			/* Advance all direct_children forward one residual. */
			while((node1 = set_next_element(n->footprint->direct_children))){
				list_next_item(node1->footprint->residual_nodes);
			}
			set_first_element(n->footprint->direct_children);
		}
	}
}

void dag_node_footprint_find_largest_residual(struct dag_node *n, struct dag_node *limit)
{
	struct dag_node *node1;

	list_first_item(n->footprint->residual_nodes);
    node1 = list_peek_current(n->footprint->residual_nodes);

	if(n != node1){
	    n->footprint->residual_size = node1->footprint->residual_size;

		set_delete(n->footprint->residual_files);
		n->footprint->residual_files = set_duplicate(node1->footprint->residual_files);
	}

    while((node1 = list_next_item(n->footprint->residual_nodes)) && (!limit || node1 != limit)){
        if(node1->footprint->footprint_min_size > n->footprint->footprint_min_size){
            set_delete(n->footprint->footprint_min_files);
            n->footprint->footprint_min_size = node1->footprint->footprint_min_size;
            n->footprint->footprint_min_files = set_duplicate(node1->footprint->footprint_min_files);
        }
        if(node1->footprint->footprint_max_size > n->footprint->footprint_max_size){
            set_delete(n->footprint->footprint_max_files);
            n->footprint->footprint_max_size = node1->footprint->footprint_max_size;
            n->footprint->footprint_max_files = set_duplicate(node1->footprint->footprint_max_files);
        }
    }
}

void dag_node_footprint_set_desc_res_wgt_diff(struct dag_node *n)
{
	struct dag_node *node1, *node2;

	set_first_element(n->footprint->direct_children);
	while((node1 = set_next_element(n->footprint->direct_children))){
		node2 = list_peek_current(node1->footprint->residual_nodes);

		/* Add the last residual's residual and terminal files in the branch
			to the current residual files */
		set_insert_set(n->footprint->residual_files, node2->footprint->residual_files);
		set_insert_set(n->footprint->residual_files, node2->footprint->terminal_files);

		/* Add the last residual's residual and terminal files in the branch
			to the branch's first node residual files */
		set_insert_set(node1->footprint->res_files, node2->footprint->residual_files);
		set_insert_set(node1->footprint->res_files, node2->footprint->terminal_files);

		/* Set branch head's res size */
		node1->footprint->res = dag_file_set_size(node1->footprint->res_files);

		set_insert_set(node1->footprint->wgt_files, node2->footprint->footprint_min_files);
		node1->footprint->wgt = node2->footprint->footprint_min_size;

		set_insert_set(node1->footprint->max_wgt_files, node2->footprint->footprint_max_files);
		node1->footprint->max_wgt = node2->footprint->footprint_max_size;

		list_next_item(node1->footprint->residual_nodes);
		while((node2 = list_peek_current(node1->footprint->residual_nodes))){
			if(node2->footprint->footprint_min_size >= node1->footprint->wgt){
				set_delete(node1->footprint->wgt_files);
				node1->footprint->wgt_files = set_duplicate(node2->footprint->footprint_min_files);
				node1->footprint->wgt = node2->footprint->footprint_min_size;
			}
			if(node2->footprint->footprint_max_size >= node1->footprint->max_wgt){
				set_delete(node1->footprint->max_wgt_files);
				node1->footprint->max_wgt_files = set_duplicate(node2->footprint->footprint_max_files);
				node1->footprint->max_wgt = node2->footprint->footprint_max_size;
			}
			list_next_item(node1->footprint->residual_nodes);
		}
	}
	n->footprint->residual_size = dag_file_set_size(n->footprint->residual_files);

	set_first_element(n->footprint->direct_children);
	while((node1 = set_next_element(n->footprint->direct_children))){
		node1->footprint->diff = node1->footprint->wgt - node1->footprint->res;
	}
}

/* The descendant footprint of a node is defined as a balance between
 * the widest point of the children branches, while still maintaining
 * the existance of the sibling branches. The assumption is that by
 * knowing the larget size needed, all other branches can be executed
 * within that designated size, so we only need to add the residual
 * size of a branch to hold onto it while the heavier weights are
 * computed. */
void dag_node_footprint_determine_descendant(struct dag_node *n)
{
	struct dag_node *node1, *node2; //, *res_node;
	struct list *tmp_direct_children = list_create();
	struct set *footprint = set_create(0);
	uint64_t footprint_size = 0;

	/* Create a second list of direct children that allows us to
		sort on footprint properties. This is used
		when we compare footprint and the residual nodes. */
	set_first_element(n->footprint->direct_children);
	while((node1 = set_next_element(n->footprint->direct_children))){
		list_push_tail(tmp_direct_children, node1);
		list_first_item(node1->footprint->residual_nodes);
	}

	/* There are two cases for descendant nodes:
		1. Multiple direct_children indicating that multiple branches will
			need to be maintained concurrently and we need to account.
		2. One descendant indicating we want to continue the chain
			of residual and footprints that out child holds.
			create empty lists for this case.
	*/
	set_first_element(n->footprint->direct_children);
	if(set_size(n->footprint->direct_children) > 1){
		dag_node_footprint_determine_desc_residual_intersect(n);

		dag_node_footprint_set_desc_res_wgt_diff(n);

		set_insert_list(footprint, n->target_files);

		list_sort(tmp_direct_children, dag_node_footprint_comp_diff);
		list_first_item(tmp_direct_children);
		/* Loop over each child giving it the chance to be the largest footprint. */
		while((node1 = list_next_item(tmp_direct_children))){
			footprint_size = dag_file_set_size(footprint);
			if((footprint_size + node1->footprint->wgt) > n->footprint->delete_footprint){
				set_delete(n->footprint->delete_files);
				n->footprint->delete_files = set_duplicate(footprint);
				set_insert_set(n->footprint->delete_files, node1->footprint->wgt_files);
				n->footprint->delete_footprint = dag_file_set_size(n->footprint->delete_files);

			}
			// This is where we would remove an input file if it wasn't needed for other branches
			set_insert_set(footprint, node1->footprint->res_files);
			list_push_tail(n->footprint->delete_run_order, node1);
		}

		list_sort(tmp_direct_children, dag_node_footprint_comp_wgt_rev);
		list_first_item(tmp_direct_children);
		node1 = list_next_item(tmp_direct_children);

		set_insert_set(n->footprint->prog_max_files, node1->footprint->max_wgt_files);
		set_insert_set(n->footprint->prog_min_files, node1->footprint->wgt_files);
		list_push_tail(n->footprint->prog_run_order, node1);

		/* Find what the total space is needed to hold all residuals and
			the largest footprint branch concurrently. */
		while((node2 = list_next_item(tmp_direct_children))){
			set_insert_set(n->footprint->prog_max_files, node2->footprint->max_wgt_files);
			set_insert_set(n->footprint->prog_min_files, node2->footprint->res_files);
			list_push_tail(n->footprint->prog_run_order, node2);
		}

		n->footprint->prog_max_footprint = dag_file_set_size(n->footprint->prog_max_files);
		n->footprint->prog_min_footprint = dag_file_set_size(n->footprint->prog_min_files);
	} else {
		if(set_size(n->footprint->direct_children) == 1){
			node1 = set_next_element(n->footprint->direct_children);
			list_delete(n->footprint->residual_nodes);
			n->footprint->residual_nodes = list_duplicate(node1->footprint->residual_nodes);
		}

		set_insert_list(n->footprint->residual_files, n->target_files);
		set_insert_set(n->footprint->residual_files, n->footprint->terminal_files);
		n->footprint->residual_size = dag_file_set_size(n->footprint->residual_files);
	}

	/* Adding the current nodes list so parents can quickly access
		these decisions. */
	list_push_tail(n->footprint->residual_nodes, n);

	list_delete(tmp_direct_children);
	set_delete(footprint);
}

/* Function that allows the purpose to be succintly stated. We only
 * store the max as the true weight of the node, so this allows it to
 * be clearly expressed. */
void dag_node_footprint_min( struct dag_node *n)
{
	set_delete(n->footprint->footprint_min_files);
	if(n->footprint->delete_footprint <= n->footprint->prog_min_footprint){
		n->footprint->footprint_min_size = n->footprint->delete_footprint;
		n->footprint->footprint_min_files = set_duplicate(n->footprint->delete_files);
		n->footprint->footprint_min_type = DAG_NODE_FOOTPRINT_DELETE;
		n->footprint->run_order = n->footprint->delete_run_order;
	} else {
		n->footprint->footprint_min_size = n->footprint->prog_min_footprint;
		n->footprint->footprint_min_files = set_duplicate(n->footprint->prog_min_files);
		n->footprint->footprint_min_type = DAG_NODE_FOOTPRINT_DESC;
		n->footprint->run_order = n->footprint->prog_run_order;
	}

	n->footprint->self_res = n->footprint->target_size;
	if(n->footprint->self_res < n->footprint->footprint_min_size){
		n->footprint->self_res = n->footprint->footprint_min_size;
	}

	if(n->footprint->run_footprint > n->footprint->footprint_min_size){
		set_delete(n->footprint->footprint_min_files);
		n->footprint->footprint_min_size = n->footprint->run_footprint;
		n->footprint->footprint_min_files = set_duplicate(n->footprint->run_files);
		n->footprint->footprint_min_type = DAG_NODE_FOOTPRINT_RUN;
	}
}

/* Function that allows the purpose to be succintly stated. We only
 * store the max as the true weight of the node, so this allows it to
 * be clearly expressed. */
void dag_node_footprint_max( struct dag_node *n)
{
	if(n->footprint->prog_max_footprint > n->footprint->footprint_max_size){
		set_delete(n->footprint->footprint_max_files);
		n->footprint->footprint_max_size = n->footprint->prog_max_footprint;
		n->footprint->footprint_max_files = set_duplicate(n->footprint->prog_max_files);
	}

	if(n->footprint->delete_footprint > n->footprint->footprint_max_size){
		set_delete(n->footprint->footprint_max_files);
		n->footprint->footprint_max_size = n->footprint->delete_footprint;
		n->footprint->footprint_max_files = set_duplicate(n->footprint->delete_files);
	}

	if(n->footprint->run_footprint > n->footprint->footprint_max_size){
		set_delete(n->footprint->footprint_max_files);
		n->footprint->footprint_max_size = n->footprint->run_footprint;
		n->footprint->footprint_max_files = set_duplicate(n->footprint->run_files);
	}
}

/* Function that calculates the three different footprint values and
 * stores the largest as the key footprint of the node. */
void dag_node_footprint_measure(struct dag_node *n)
{
	struct dag_node *c;

	dag_node_footprint_determine_run_footprint(n);

	/* Have un-updated children calculate their current footprint. */
	set_first_element(n->footprint->direct_children);
	while((c = set_next_element(n->footprint->direct_children))){
		if(!c->footprint->footprint_updated)
			dag_node_footprint_measure(c);
	}

	dag_node_footprint_determine_descendant(n);

	/* Finds the max of all three different weights. */
	dag_node_footprint_min(n);

	/* Finds the max of all three different weights. */
	dag_node_footprint_max(n);

	/* Mark node as having been updated. */
	n->footprint->footprint_updated = 1;
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
		fprintf(out, "%"PRIu64"%s", n->footprint->footprint_min_size, delim);
		fprintf(out, "%"PRIu64"%s", n->footprint->footprint_max_size, delim);
		fprintf(out, "%"PRIu64"%s", n->footprint->residual_size, delim);
		fprintf(out, "%"PRIu64"%s", n->footprint->run_footprint, delim);
		fprintf(out, "%"PRIu64"%s", n->footprint->delete_footprint, delim);
		fprintf(out, "%"PRIu64"%s", n->footprint->prog_min_footprint, delim);
		fprintf(out, "%"PRIu64"%s", n->footprint->prog_max_footprint, node_retrn);
		if(symbolic) {
			dag_node_print_node_list(n->footprint->residual_nodes, out, delim);
		} else {
			dag_node_print_node_list(n->footprint->residual_nodes, out, delim);
			fprintf(out,"%s%s%s%s%s%s%s",
				delim,delim,delim,delim,delim,delim,retrn);
		}
	}

	if(symbolic){
		dag_node_print_file_set(n->footprint->footprint_min_files, out, delim);
		dag_node_print_file_set(n->footprint->footprint_max_files, out, delim);
		dag_node_print_file_set(n->footprint->residual_files, out, delim);
		dag_node_print_file_set(n->footprint->run_files, out, delim);
		dag_node_print_file_set(n->footprint->delete_files, out, delim);
		dag_node_print_file_set(n->footprint->prog_min_files, out, delim);
		dag_node_print_file_set(n->footprint->prog_max_files, out, retrn);
	}
	
}

void dag_node_footprint_print(struct dag *d, struct dag_node *base, char *output)
{
	struct dag_node *n;

	int tex = 0;

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
	fprintf(out, "Base %s %"PRIu64" %s %"PRIu64" %s %"PRIu64"%s%s%s%s%s",
				delim, base->footprint->footprint_min_size, delim, base->footprint->footprint_max_size,
				delim, base->footprint->residual_size, delim,delim,delim,delim,node_retrn);

	list_pop_tail(base->footprint->residual_nodes);
	dag_node_print_node_list(base->footprint->residual_nodes, out, delim);
	dag_node_print_file_set(base->footprint->footprint_min_files, out, delim);
	dag_node_print_file_set(base->footprint->footprint_max_files, out, delim);
	dag_node_print_file_set(base->footprint->residual_files, out, delim);
	fprintf(out,"%s%s%s%s",
		delim,delim,delim,retrn);

	if(tex)
		fprintf(out, "\\end{tabular}\n");

	fclose(out);
}

/* After a node has been completed mark that it and its
 * children are in need of being updated. */
void dag_node_footprint_reset(struct dag_node *n)
{
	struct dag_node *n1;
	set_first_element(n->footprint->direct_children);
	while((n1 = set_next_element(n->footprint->direct_children))){
		if(n1->footprint->footprint_updated || !n->footprint->terminal_updated)
			dag_node_footprint_reset(n1);
	}
	n->footprint->size_updated = 0;
	n->footprint->footprint_updated = 0;
	n->footprint->terminal_updated = set_size(n->ancestors);
}

int dag_node_footprint_dependencies_active(struct dag_node *n)
{
	return 1;
	if(!n->footprint->dependencies)
		return 1;

	struct dag_node *n1;
	set_first_element(n->footprint->dependencies);
	while((n1 = set_next_element(n->footprint->dependencies))){
		if(!(n1->state == DAG_NODE_STATE_RUNNING || n1->state == DAG_NODE_STATE_COMPLETE)){
			return 0;
		}
	}

	return 1;
}


void dag_node_footprint_calculate(struct dag_node *n){
	dag_node_footprint_determine_children(n);
	dag_node_footprint_reset(n);
	dag_node_footprint_prepare_node_terminal_files(n);
	dag_node_footprint_prepare_node_size(n);
	dag_node_footprint_measure(n);
}

/* vim: set noexpandtab tabstop=4: */
