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

int dag_node_comp(void *item, const void *arg)
{
	struct dag_node *d = ((struct dag_node *) item);
	struct dag_node *e = ((struct dag_node *) arg);

	if(d->nodeid == e->nodeid)
		return 1;
	return 0;
}

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

	n->source_files = list_create();
	n->target_files = list_create();

	n->remote_names = itable_create(0);
	n->remote_names_inv = hash_table_create(0, 0);

	n->descendants = set_create(0);
	n->ancestors = set_create(0);

	n->source_size = -1;
	n->target_size = -1;

	n->res_nodes = list_create();
	n->wgt_nodes = list_create();
	n->run_nodes = list_create();

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

struct dag_node_size *dag_node_size_create(struct dag_node *n, uint64_t size)
{
	struct dag_node_size *s;

	s = malloc(sizeof(struct dag_node_size));
	s->n = n;
	s->size = size;

	return s;
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
void dag_node_add_target_file(struct dag_node *n, const char *filename, const char *remotename)
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

void dag_node_prepare_node_size(struct dag_node *n)
{
	struct dag_file *f;
	struct dag_node *s;
	n->source_size = 0;
	n->target_size = 0;
	list_first_item(n->source_files);
	while((f = list_next_item(n->source_files))){
		if(dag_file_should_exist(f))
			n->source_size += f->file_size;
		else
			n->source_size += f->est_size;
	}
	list_first_item(n->target_files);
	while((f = list_next_item(n->target_files))){
		if(dag_file_should_exist(f))
			n->target_size += f->file_size;
		else
			n->target_size += f->est_size;
	}

	set_first_element(n->descendants);
	while((s = set_next_element(n->descendants))){
		dag_node_prepare_node_size(s);
	}
}

uint64_t dag_node_determine_parent_weight(struct dag_node *n)
{
	struct dag_node *d;
	uint64_t weight = n->target_size;
	set_first_element(n->ancestors);
	while((d = set_next_element(n->ancestors))){
		weight += d->target_size;
	}

	return weight;
}

uint64_t dag_node_determine_child_weight(struct dag_node *n)
{
	struct dag_node *d;
	struct dag_file *f;
	uint64_t weight = n->target_size;
	set_first_element(n->descendants);
	while((d = set_next_element(n->descendants))){
		weight += d->target_size;
		while((f = list_next_item(n->source_files))){
			if(f->created_by->nodeid == n->nodeid)
				continue;
			weight += dag_file_size(f);
		}
	}

	return weight;
}

uint64_t dag_node_determine_descendant_weight(struct dag_node *n)
{
	struct dag_node *d, *e;
	struct dag_node_size *s, *t;
	struct set *tmp = set_create(0);

	set_first_element(n->descendants);
	while((d = set_next_element(n->descendants))){
		set_push(tmp, d);
		list_first_item(d->res_nodes);
		list_first_item(d->wgt_nodes);
	}

	list_delete(n->res_nodes);
	list_delete(n->wgt_nodes);
	list_delete(n->run_nodes);

	set_first_element(n->descendants);
	if(set_size(n->descendants) > 1){
		n->res_nodes = list_create();
		n->wgt_nodes = list_create();
		n->run_nodes = list_create();
		set_first_element(n->descendants);

		int comp = 1;
		while(comp){
			d = set_next_element(n->descendants);
			s = list_next_item(d->res_nodes);
			while((d = set_next_element(n->descendants))){
				t = list_next_item(d->res_nodes);
				if((t && !s) || (s && !t) || (t->n->nodeid != s->n->nodeid))
					comp = 0;
			}

			set_first_element(n->descendants);
			if(comp)
				list_push_tail(n->res_nodes, s);
		}

		comp = 1;
		while(comp){
			d = set_next_element(n->descendants);
			s = list_next_item(d->wgt_nodes);
			while((d = set_next_element(n->descendants))){
				t = list_next_item(d->wgt_nodes);
				if((t && !s) || (s && !t) || (t->n->nodeid != s->n->nodeid))
					comp = 0;
			}

			set_first_element(n->descendants);
			if(comp)
				list_push_tail(n->wgt_nodes, s);
		}

		uint64_t node_wgt, max_wgt, tmp_wgt, res_size;
		max_wgt = 0;
		set_first_element(tmp);
		while((d = set_next_element(tmp))){
			struct list *tmp_run = list_create();
			node_wgt = d->parent_wgt;
			if(d->parent_wgt <= d->child_wgt)
				node_wgt = d->child_wgt;
			list_push_head(tmp_run, d);

			while((t = list_peek_current(d->wgt_nodes))){
				if(t->size > node_wgt)
					node_wgt = t->size;
				list_next_item(d->wgt_nodes);
			}
			tmp_wgt = node_wgt;
			set_first_element(n->descendants);
			while((e = set_next_element(n->descendants))){
				t = list_peek_current(e->res_nodes);
				if(!t)
					res_size = e->target_size;
				else
					res_size = t->size;
				if(e->nodeid != d->nodeid){
					node_wgt += res_size;
					list_push_head(tmp_run, e);
				}
			}

			if(max_wgt < tmp_wgt || (max_wgt == tmp_wgt && node_wgt < n->descendant_wgt)){
				max_wgt = tmp_wgt;
				n->descendant_wgt = node_wgt;
				list_delete(n->run_nodes);
				n->run_nodes = list_duplicate(tmp_run);
			}
			list_delete(tmp_run);
		}
		return max_wgt;
	} else if(set_size(n->descendants) == 1){
		d = set_next_element(n->descendants);
		n->run_nodes = list_create();
		list_push_tail(n->run_nodes, d);

		n->res_nodes = list_duplicate(d->res_nodes);
		n->wgt_nodes = list_duplicate(d->wgt_nodes);
	} else {
		n->res_nodes = list_create();
		n->wgt_nodes = list_create();
		n->run_nodes = list_create();
	}
	return 0;
}

void dag_node_determine_footprint(struct dag_node *n)
{
	struct dag_node *d;
	struct dag_node_size *s;

	n->parent_wgt = dag_node_determine_parent_weight(n);

	n->child_wgt = dag_node_determine_child_weight(n);

	set_first_element(n->descendants);
	while((d = set_next_element(n->descendants))){
		dag_node_determine_footprint(d);
	}

	n->descendant_wgt = dag_node_determine_descendant_weight(n);

	list_push_tail(n->res_nodes, dag_node_size_create(n, n->target_size));

	if((n->parent_wgt >= n->child_wgt) && (n->parent_wgt >= n->descendant_wgt)){
		list_push_tail(n->wgt_nodes, dag_node_size_create(n, n->parent_wgt));
	} else if((n->child_wgt >= n->parent_wgt) && (n->child_wgt >= n->descendant_wgt)){
		list_push_tail(n->wgt_nodes, dag_node_size_create(n, n->child_wgt));
	} else {
		list_push_tail(n->wgt_nodes, dag_node_size_create(n, n->descendant_wgt));
	}

/*		USED FOR CHECKING EXPECTED SIZES
*/
	printf("Parent weight for %d : %" PRIu64"\n", n->nodeid, n->parent_wgt);
	printf("Child weight for %d : %" PRIu64"\n", n->nodeid, n->child_wgt);
	printf("Desc weight for %d : %" PRIu64"\n", n->nodeid, n->descendant_wgt);

	list_first_item(n->run_nodes);
	while((d = list_next_item(n->run_nodes)))
		printf("%d\t", d->nodeid);
	printf("\n");

	list_first_item(n->wgt_nodes);
	while((s = list_next_item(n->wgt_nodes))){
		printf("(%d, %"PRIu64") ", s->n->nodeid, s->size);
	}
	printf("\n");

	list_first_item(n->res_nodes);
	while((s = list_next_item(n->res_nodes))){
		printf("(%d, %"PRIu64") ", s->n->nodeid, s->size);
	}
	printf("\n");fflush(stdout);
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
