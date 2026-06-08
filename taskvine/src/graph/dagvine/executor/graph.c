#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "debug.h"
#include "graph.h"
#include "priority_queue.h"
#include "set.h"
#include "stringtools.h"
#include "uuid.h"
#include "xxmalloc.h"

/*************************************************************/
/* Private Functions */
/*************************************************************/

/**
 * Compute a topological ordering of the executor graph.
 * Call only after all nodes, edges, and metrics have been populated.
 * @param g Reference to the executor graph.
 * @return Nodes in topological order.
 */
static struct list *get_topological_order(struct graph *g)
{
	if (!g) {
		return NULL;
	}

	int total_nodes = itable_size(g->nodes);
	struct list *topo_order = list_create();
	struct itable *in_degree_map = itable_create(0);
	struct priority_queue *pq = priority_queue_create(total_nodes);

	uint64_t nid;
	struct node *node;
	ITABLE_ITERATE(g->nodes, nid, node)
	{
		int deg = list_size(node->parents);
		itable_insert(in_degree_map, nid, (void *)(intptr_t)deg);
		if (deg == 0) {
			priority_queue_push(pq, node, -(double)node->node_id);
		}
	}

	while (priority_queue_size(pq) > 0) {
		struct node *current = priority_queue_pop(pq);
		list_push_tail(topo_order, current);

		struct node *child;
		LIST_ITERATE(current->children, child)
		{
			intptr_t raw_deg = (intptr_t)itable_lookup(in_degree_map, child->node_id);
			int deg = (int)raw_deg - 1;
			itable_insert(in_degree_map, child->node_id, (void *)(intptr_t)deg);

			if (deg == 0) {
				priority_queue_push(pq, child, -(double)child->node_id);
			}
		}
	}

	if (list_size(topo_order) != total_nodes) {
		debug(D_ERROR, "Error: executor graph contains cycles or is malformed.");
		debug(D_ERROR, "Expected %d nodes, but only sorted %d.", total_nodes, list_size(topo_order));

		uint64_t id;
		ITABLE_ITERATE(g->nodes, id, node)
		{
			intptr_t raw_deg = (intptr_t)itable_lookup(in_degree_map, id);
			int deg = (int)raw_deg;
			if (deg > 0) {
				debug(D_ERROR, "  Node %" PRIu64 " has in-degree %d. Parents:", id, deg);
				struct node *p;
				LIST_ITERATE(node->parents, p)
				{
					debug(D_ERROR, "    -> %" PRIu64, p->node_id);
				}
			}
		}

		list_delete(topo_order);
		itable_delete(in_degree_map);
		priority_queue_delete(pq);
		exit(1);
	}

	itable_delete(in_degree_map);
	priority_queue_delete(pq);
	return topo_order;
}

/**
 * Extract weakly connected components of the executor graph.
 * Currently used for debugging and instrumentation only.
 * @param g Reference to the executor graph.
 * @return List of weakly connected components.
 */
static struct list *extract_weakly_connected_components(struct graph *g)
{
	if (!g) {
		return NULL;
	}

	struct set *visited = set_create(0);
	struct list *components = list_create();

	uint64_t nid;
	struct node *node;
	ITABLE_ITERATE(g->nodes, nid, node)
	{
		if (set_lookup(visited, node)) {
			continue;
		}

		struct list *component = list_create();
		struct list *queue = list_create();

		list_push_tail(queue, node);
		set_insert(visited, node);
		list_push_tail(component, node);

		while (list_size(queue) > 0) {
			struct node *curr = list_pop_head(queue);

			struct node *p;
			LIST_ITERATE(curr->parents, p)
			{
				if (!set_lookup(visited, p)) {
					list_push_tail(queue, p);
					set_insert(visited, p);
					list_push_tail(component, p);
				}
			}

			struct node *c;
			LIST_ITERATE(curr->children, c)
			{
				if (!set_lookup(visited, c)) {
					list_push_tail(queue, c);
					set_insert(visited, c);
					list_push_tail(component, c);
				}
			}
		}

		list_push_tail(components, component);
		list_delete(queue);
	}

	set_delete(visited);
	return components;
}

/**
 * Compute the heavy score of a node in the executor graph.
 * @param node Reference to the node.
 * @return Heavy score.
 */
static double compute_node_heavy_score(struct node *node)
{
	if (!node) {
		return 0;
	}

	double up_score = node->depth * node->upstream_subgraph_size * node->fan_in;
	double down_score = node->height * node->downstream_subgraph_size * node->fan_out;

	return up_score / (down_score + 1);
}

static void assign_node_outfile_local(struct node *node)
{
	node->outfile_type = NODE_OUTFILE_TYPE_LOCAL;
}

static void assign_node_outfile_temp(struct node *node)
{
	node->outfile_type = NODE_OUTFILE_TYPE_TEMP;
}

/**
 * Compute upstream/downstream subgraph sizes and heavy scores for each node.
 * This is expensive (can approach transitive-closure cost) and should only be
 * invoked when heavy-score-based checkpoint selection is enabled.
 */
static void compute_upstream_downstream_and_heavy_scores(struct graph *g, struct list *topo_order)
{
	if (!g || !topo_order) {
		return;
	}

	struct node *node;
	struct node *parent_node;
	struct node *child_node;

	struct itable *upstream_map = itable_create(0);	  // reachable ancestors per node
	struct itable *downstream_map = itable_create(0); // reachable descendants per node
	uint64_t nid_tmp;
	ITABLE_ITERATE(g->nodes, nid_tmp, node)
	{
		struct set *upstream = set_create(0);
		struct set *downstream = set_create(0);
		itable_insert(upstream_map, node->node_id, upstream);
		itable_insert(downstream_map, node->node_id, downstream);
	}

	LIST_ITERATE(topo_order, node)
	{
		struct set *upstream = itable_lookup(upstream_map, node->node_id);
		LIST_ITERATE(node->parents, parent_node)
		{
			struct set *parent_upstream = itable_lookup(upstream_map, parent_node->node_id);
			set_insert_set(upstream, parent_upstream); // in-place union, not set_union
			set_insert(upstream, parent_node);
		}
	}

	LIST_ITERATE_REVERSE(topo_order, node)
	{
		struct set *downstream = itable_lookup(downstream_map, node->node_id);
		LIST_ITERATE(node->children, child_node)
		{
			struct set *child_downstream = itable_lookup(downstream_map, child_node->node_id);
			set_insert_set(downstream, child_downstream); // in-place union, not set_union
			set_insert(downstream, child_node);
		}
	}

	LIST_ITERATE(topo_order, node)
	{
		node->upstream_subgraph_size = set_size(itable_lookup(upstream_map, node->node_id));
		node->downstream_subgraph_size = set_size(itable_lookup(downstream_map, node->node_id));
		node->fan_in = list_size(node->parents);
		node->fan_out = list_size(node->children);
		set_delete(itable_lookup(upstream_map, node->node_id));
		set_delete(itable_lookup(downstream_map, node->node_id));
	}

	itable_delete(upstream_map);
	itable_delete(downstream_map);

	LIST_ITERATE(topo_order, node)
	{
		node->heavy_score = compute_node_heavy_score(node); // ranks checkpoint candidates
	}
}

/*************************************************************/
/* Public APIs */
/*************************************************************/

/** Tune the executor graph.
 * @param g Reference to the executor graph.
 * @param name Reference to the name of the parameter to tune.
 * @param value Reference to the value of the parameter to tune.
 * @return 0 on success, -1 on failure.
 */
int graph_tune(struct graph *g, const char *name, const char *value)
{
	if (!g || !name || !value) {
		return -1;
	}

	if (strcmp(name, "output-dir") == 0) {
		if (mkdir(value, 0777) != 0 && errno != EEXIST) {
			debug(D_ERROR, "failed to mkdir %s (errno=%d)", value, errno);
			return -1;
		}
		free(g->output_dir);
		g->output_dir = xxstrdup(value);

	} else if (strcmp(name, "prune-depth") == 0) {
		int k = atoi(value);
		if (k < 0) {
			debug(D_ERROR, "invalid prune-depth: %s (must be >= 0; 0 disables prune-depth release)", value);
			return -1;
		}
		g->prune_depth = k;

	} else if (strcmp(name, "checkpoint-fraction") == 0) {
		double fraction = atof(value);
		if (fraction < 0.0 || fraction > 1.0) {
			debug(D_ERROR, "invalid checkpoint fraction: %s (must be between 0.0 and 1.0)", value);
			return -1;
		}
		g->checkpoint_fraction = fraction;

	} else if (strcmp(name, "checkpoint-dir") == 0) {
		if (mkdir(value, 0777) != 0 && errno != EEXIST) {
			debug(D_ERROR, "failed to mkdir %s (errno=%d)", value, errno);
			return -1;
		}
		free(g->checkpoint_dir);
		g->checkpoint_dir = xxstrdup(value);

	} else if (strcmp(name, "print-graph-details") == 0) {
		g->print_graph_details = (atoi(value) == 1) ? 1 : 0;
	} else if (strcmp(name, "chain-grouping-enabled") == 0) {
		/* Stays aligned with Python's --task-group. When zero the executor never merges chain members. */
		g->chain_grouping_enabled = (atoi(value) != 0) ? 1 : 0;
	} else {
		debug(D_ERROR, "invalid parameter name: %s", name);
		return -1;
	}

	return 0;
}

/**
 * Get the outfile remote name of a node in the executor graph.
 * @param g Reference to the executor graph.
 * @param node_id Reference to the node id.
 * @return The outfile remote name.
 */
const char *graph_get_node_outfile_remote_name(const struct graph *g, uint64_t node_id)
{
	if (!g) {
		return NULL;
	}

	struct node *node = itable_lookup(g->nodes, node_id);
	if (!node) {
		return NULL;
	}

	return node->outfile_remote_name;
}

/**
 * Get the task runner library name of the executor graph.
 * @param g Reference to the executor graph.
 * @return The task runner library name.
 */
const char *graph_get_task_runner_library_name(const struct graph *g)
{
	if (!g) {
		return NULL;
	}

	return g->task_runner_library_name;
}

/**
 * Set the task runner function name of the executor graph.
 * @param g Reference to the executor graph.
 * @param task_runner_function_name Reference to the task runner function name.
 */
void graph_set_task_runner_function_name(struct graph *g, const char *task_runner_function_name)
{
	if (!g || !task_runner_function_name) {
		return;
	}

	if (g->task_runner_function_name) {
		free(g->task_runner_function_name);
	}

	g->task_runner_function_name = xxstrdup(task_runner_function_name);
}

/**
 * Get the heavy score of a node in the executor graph.
 * @param g Reference to the executor graph.
 * @param node_id Reference to the node id.
 * @return The heavy score.
 */
double graph_get_node_heavy_score(const struct graph *g, uint64_t node_id)
{
	if (!g) {
		return -1;
	}

	struct node *node = itable_lookup(g->nodes, node_id);
	if (!node) {
		return -1;
	}

	return node->heavy_score;
}

/**
 * Compute the topology metrics of the executor graph, including depth, height, upstream and downstream counts,
 * heavy scores, and weakly connected components. Must be called after all nodes and dependencies are added.
 * @param g Reference to the executor graph.
 */
void graph_finalize(struct graph *g)
{
	if (!g) {
		return;
	}

	struct list *topo_order = get_topological_order(g); // required for all metric passes
	if (!topo_order) {
		return;
	}

	struct node *node;
	struct node *parent_node;
	struct node *child_node;

	/* Longest path from any source in topo order. */
	LIST_ITERATE(topo_order, node)
	{
		node->depth = 0;
		LIST_ITERATE(node->parents, parent_node)
		{
			if (node->depth < parent_node->depth + 1) {
				node->depth = parent_node->depth + 1;
			}
		}
	}

	/* Longest path to any sink in reverse topo order. */
	LIST_ITERATE_REVERSE(topo_order, node)
	{
		node->height = 0;
		LIST_ITERATE(node->children, child_node)
		{
			if (node->height < child_node->height + 1) {
				node->height = child_node->height + 1;
			}
		}
	}

	int total_nodes = list_size(topo_order);
	int total_target_nodes = 0;
	LIST_ITERATE(topo_order, node)
	{
		if (node->is_target) {
			total_target_nodes++;
		}
	}

	/*
	 * Pick how many non-target nodes become shared-filesystem checkpoints.
	 * If zero, skip heavy-score passes entirely.
	 */
	int checkpoint_count = (int)((total_nodes - total_target_nodes) * g->checkpoint_fraction);
	if (checkpoint_count < 0) {
		checkpoint_count = 0;
	}

	if (checkpoint_count > 0) {
		compute_upstream_downstream_and_heavy_scores(g, topo_order); // expensive, only if ranking needed

		struct priority_queue *sorted_nodes = priority_queue_create(total_nodes);
		LIST_ITERATE(topo_order, node)
		{
			priority_queue_push(sorted_nodes, node, node->heavy_score);
		}

		int assigned_checkpoint_count = 0;
		while ((node = priority_queue_pop(sorted_nodes))) {
			if (node->is_target) {
				assign_node_outfile_local(node); // targets keep managed local returns
				continue;
			}
			if (assigned_checkpoint_count < checkpoint_count) {
				/*
				 * Top heavy_score nodes checkpoint to shared storage under checkpoint_dir.
				 * No vine_file handle for that mode.
				 */
				node->outfile_type = NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM;
				char *shared_file_system_outfile_path = string_format("%s/%s", g->checkpoint_dir, node->outfile_remote_name);
				free(node->outfile_remote_name);
				node->outfile_remote_name = shared_file_system_outfile_path;
				assigned_checkpoint_count++;
			} else {
				assign_node_outfile_temp(node); // remaining nodes use temp storage
			}
		}
		priority_queue_delete(sorted_nodes);
	} else {
		LIST_ITERATE(topo_order, node)
		{
			if (node->is_target) {
				assign_node_outfile_local(node);
			} else {
				assign_node_outfile_temp(node); // no checkpoint budget, all non-targets are temp
			}
		}
	}

	if (g->print_graph_details) {
		// weakly connected components and node_debug_print, debug only
		struct list *weakly_connected_components = extract_weakly_connected_components(g);
		struct list *component;
		int component_index = 0;
		debug(D_VINE, "graph has %d weakly connected components\n", list_size(weakly_connected_components));
		LIST_ITERATE(weakly_connected_components, component)
		{
			debug(D_VINE, "component %d size: %d\n", component_index, list_size(component));
			list_delete(component);
			component_index++;
		}
		list_delete(weakly_connected_components);

		LIST_ITERATE(topo_order, node)
		{
			node_debug_print(node);
		}
	}

	list_delete(topo_order);

	return;
}

/**
 * Create a new node and track it in the executor graph.
 * @param g Reference to the executor graph.
 * @return The auto-assigned node id.
 */
uint64_t graph_add_node(struct graph *g)
{
	if (!g) {
		return 0;
	}

	uint64_t candidate_id = itable_size(g->nodes);
	candidate_id += 1; // skip zero, search upward until unused
	while (itable_lookup(g->nodes, candidate_id)) {
		candidate_id++;
	}
	uint64_t node_id = candidate_id;

	struct node *node = node_create(node_id); // defaults to non-target

	if (!node) {
		debug(D_ERROR, "failed to create node %" PRIu64, node_id);
		graph_delete(g);
		exit(1);
	}

	itable_insert(g->nodes, node_id, node);

	return node_id;
}

/**
 * Mark a node as a retrieval target.
 */
void graph_set_target(struct graph *g, uint64_t node_id)
{
	if (!g) {
		return;
	}
	struct node *node = itable_lookup(g->nodes, node_id);
	if (!node) {
		debug(D_ERROR, "node %" PRIu64 " not found", node_id);
		exit(1);
	}

	node->is_target = 1;
}

/**
 * Create a new executor graph using graph-owned path configuration.
 * @param runtime_dir Runtime directory used as the default path root.
 * @return A new executor graph instance.
 */
struct graph *graph_create(const char *runtime_dir)
{
	if (!runtime_dir) {
		return NULL;
	}

	struct graph *g = xxmalloc(sizeof(struct graph));

	g->checkpoint_dir = xxstrdup(runtime_dir); // default to current working directory
	g->output_dir = xxstrdup(runtime_dir);	   // default to current working directory

	g->nodes = itable_create(0);
	g->super_leader_to_members = itable_create(0);
	g->outfile_cachename_to_node = hash_table_create(0, 0);
	g->inout_filename_to_cached_name = hash_table_create(0, 0);
	g->supernode_leader_child_to_input_source = hash_table_create(0, 0);

	cctools_uuid_t task_runner_library_name_id;
	cctools_uuid_create(&task_runner_library_name_id);
	g->task_runner_library_name = xxstrdup(task_runner_library_name_id.str);

	g->task_runner_function_name = NULL;
	g->checkpoint_fraction = 0.0;

	/* Default prune-depth: release a TEMP node as soon as all of its
	 * direct children have completed. Set to 0 via tune("prune-depth") to
	 * disable and rely exclusively on cut-propagation. */
	g->prune_depth = 1;

	g->print_graph_details = 0;
	g->chain_grouping_enabled = 0;

	return g;
}

/**
 * Add a dependency between two nodes in the executor graph. Note that the input-output file relationship
 * is not handled here, because their file names might not have been determined yet.
 * @param g Reference to the executor graph.
 * @param parent_id Reference to the parent node id.
 * @param child_id Reference to the child node id.
 */
void graph_add_dependency(struct graph *g, uint64_t parent_id, uint64_t child_id)
{
	if (!g) {
		return;
	}

	struct node *parent_node = itable_lookup(g->nodes, parent_id);
	struct node *child_node = itable_lookup(g->nodes, child_id);
	if (!parent_node) {
		debug(D_ERROR, "parent node %" PRIu64 " not found", parent_id);
		exit(1);
	}
	if (!child_node) {
		debug(D_ERROR, "child node %" PRIu64 " not found", child_id);
		exit(1);
	}

	node_ensure_dependency(parent_node, child_node);

	return;
}

/**
 * Copy a list of struct node * into a new list in the same order (pointer values only, not deep copy).
 * Snapshots parents/children before mutating edges during supernode rewire. Caller frees the new list.
 */
static struct list *graph_copy_node_ptr_list(struct list *src)
{
	struct list *dst = list_create();
	if (!src) {
		return dst;
	}
	struct node *x;
	LIST_ITERATE(src, x)
	{
		list_push_tail(dst, x);
	}
	return dst;
}

/**
 * After supernode registration: for each node in mset, remove internal edges (both ends in mset).
 * Rewire edges that cross the group boundary through leader. mset contains leader plus all non-leader members.
 */
static void graph_supernode_rewire(struct graph *g, struct node *leader, struct set *mset)
{
	struct node *m;
	uint64_t nid;
	ITABLE_ITERATE(g->nodes, nid, m)
	{
		if (!set_lookup(mset, m)) {
			continue;
		}

		struct list *psnap = graph_copy_node_ptr_list(m->parents);
		struct node *p;
		while ((p = list_pop_head(psnap))) {
			if (set_lookup(mset, p)) {
				node_remove_dependency(p, m);
			} else {
				node_remove_dependency(p, m);
				node_ensure_dependency(p, leader);
			}
		}
		list_delete(psnap);

		struct list *csnap = graph_copy_node_ptr_list(m->children);
		struct node *c;
		while ((c = list_pop_head(csnap))) {
			if (set_lookup(mset, c)) {
				node_remove_dependency(m, c);
			} else {
				node_remove_dependency(m, c);
				node_ensure_dependency(leader, c);
				/*
				 * Scheduling sees leader->c; data still flows from member m (e.g. chain tail).
				 * Record so materialize mounts m->outfile, not the leader's primary outfile.
				 */
				if (m != leader) {
					char *lckey = string_format("%" PRIu64 ",%" PRIu64, leader->node_id, c->node_id);
					/*
					 * The table keeps the first insert if the same key is stored twice. Drop the
					 * old mapping explicitly so the node that truly produces the file for c wins.
					 */
					hash_table_remove(g->supernode_leader_child_to_input_source, lckey);
					hash_table_insert(g->supernode_leader_child_to_input_source, lckey, m);
					free(lckey);
				}
			}
		}
		list_delete(csnap);
	}
}

/**
 * Resolve the leader struct node * for any node in a supernode (singleton maps to itself).
 * Returns NULL if g or n is NULL or the leader id is missing from g.
 */
struct node *graph_supernode_leader_node(struct graph *g, struct node *n)
{
	if (!g || !n) {
		return NULL;
	}
	return itable_lookup(g->nodes, n->super_leader_id);
}

/**
 * List of non-leader member nodes for leader_id. Owned by the graph; do not free.
 * Items are struct node *. NULL if g is NULL or there is no entry.
 */
struct list *graph_supernode_nonleader_members(struct graph *g, uint64_t leader_id)
{
	if (!g) {
		return NULL;
	}
	return itable_lookup(g->super_leader_to_members, leader_id);
}

/**
 * Register a supernode: validate members, rewire externals to leader_id, set super_leader_id on all members,
 * clear fired_parents, and store non-leaders in super_leader_to_members. Nodes and plain deps must exist first.
 * Returns 0 on success, -1 on error (see debug log).
 */
int graph_supernode_register(struct graph *g, uint64_t leader_id, const uint64_t *member_ids, int n_member_ids)
{
	if (!g || member_ids == NULL || n_member_ids < 1) {
		debug(D_ERROR, "graph_supernode_register: invalid arguments");
		return -1;
	}

	struct node *leader = itable_lookup(g->nodes, leader_id);
	if (!leader) {
		debug(D_ERROR, "graph_supernode_register: leader %" PRIu64 " not found", leader_id);
		return -1;
	}

	struct set *seen = set_create(0);
	struct set *mset = set_create(0);
	set_insert(mset, leader);

	for (int i = 0; i < n_member_ids; i++) {
		uint64_t mid = member_ids[i];
		if (mid == leader_id) {
			debug(D_ERROR, "graph_supernode_register: member list must not contain leader %" PRIu64, leader_id);
			set_delete(seen);
			set_delete(mset);
			return -1;
		}
		if (set_lookup(seen, (void *)(uintptr_t)mid)) {
			debug(D_ERROR, "graph_supernode_register: duplicate member %" PRIu64, mid);
			set_delete(seen);
			set_delete(mset);
			return -1;
		}

		struct node *mn = itable_lookup(g->nodes, mid);
		if (!mn) {
			debug(D_ERROR, "graph_supernode_register: member node %" PRIu64 " not found", mid);
			set_delete(seen);
			set_delete(mset);
			return -1;
		}
		if (mn->super_leader_id != mn->node_id) {
			debug(D_ERROR,
					"graph_supernode_register: node %" PRIu64 " already belongs to leader %" PRIu64,
					mid,
					mn->super_leader_id);
			set_delete(seen);
			set_delete(mset);
			return -1;
		}
		set_insert(seen, (void *)(uintptr_t)mid);
		set_insert(mset, mn);
	}
	set_delete(seen);

	if (leader->super_leader_id != leader->node_id) {
		debug(D_ERROR,
				"graph_supernode_register: leader %" PRIu64 " already belongs to group %" PRIu64,
				leader_id,
				leader->super_leader_id);
		set_delete(mset);
		return -1;
	}

	if (itable_lookup(g->super_leader_to_members, leader_id)) {
		debug(D_ERROR, "graph_supernode_register: leader %" PRIu64 " already registered", leader_id);
		set_delete(mset);
		return -1;
	}

	graph_supernode_rewire(g, leader, mset);

	struct node *x;
	uint64_t xid;
	ITABLE_ITERATE(g->nodes, xid, x)
	{
		if (set_lookup(mset, x)) {
			x->super_leader_id = leader_id;
			node_clear_fired_parents(x);
		}
	}

	struct list *nonleaders = list_create();
	for (int i = 0; i < n_member_ids; i++) {
		struct node *mn = itable_lookup(g->nodes, member_ids[i]);
		list_push_tail(nonleaders, mn);
	}
	itable_insert(g->super_leader_to_members, leader_id, nonleaders);

	set_delete(mset);
	return 0;
}

/** Exactly one child pointer, or NULL if not exactly one. */
static struct node *graph_node_only_child(struct node *n)
{
	if (!n || list_size(n->children) != 1) {
		return NULL;
	}
	struct node *c;
	LIST_ITERATE(n->children, c)
	{
		return c;
	}
	return NULL;
}

/** Exactly one parent pointer, or NULL if not exactly one. */
static struct node *graph_node_only_parent(struct node *n)
{
	if (!n || list_size(n->parents) != 1) {
		return NULL;
	}
	struct node *p;
	LIST_ITERATE(n->parents, p)
	{
		return p;
	}
	return NULL;
}

/**
 * Head of a maximal linear chain: singleton, and not strictly inside such a chain from the left
 * (no parent, fan-in > 1, or unique parent has fan-out > 1).
 */
static int graph_node_is_chain_head(struct node *n)
{
	if (!graph_node_is_supernode_leader(n)) {
		return 0;
	}
	if (list_size(n->parents) == 0) {
		return 1;
	}
	if (list_size(n->parents) > 1) {
		return 1;
	}
	struct node *p = graph_node_only_parent(n);
	return p && list_size(p->children) > 1;
}

struct pending_chain_group {
	uint64_t leader_id;
	uint64_t *member_ids;
	int n_members;
};

int graph_group_chain_like_tasks(struct graph *g)
{
	if (!g) {
		return -1;
	}
	if (!g->chain_grouping_enabled) {
		return 0;
	}

	/*
	 * Registering a supernode rewires the graph. Find every chain on the original adjacency
	 * first, then register them, so a half-updated graph does not confuse later chain walks.
	 */
	struct list *pending = list_create();

	uint64_t nid;
	struct node *n;
	ITABLE_ITERATE(g->nodes, nid, n)
	{
		if (!graph_node_is_chain_head(n)) {
			continue;
		}
		if (list_size(n->children) != 1) {
			continue;
		}

		struct list *chain = list_create();
		struct node *cur = n;
		for (;;) {
			list_push_tail(chain, cur);
			if (list_size(cur->children) != 1) {
				break;
			}
			struct node *c = graph_node_only_child(cur);
			if (!c || !graph_node_is_supernode_leader(c)) {
				break;
			}
			if (c->is_target) {
				/* Leave the retrieval or output consumer as its own task outside the merged chain. */
				break;
			}
			if (list_size(c->parents) != 1) {
				break;
			}
			if (graph_node_only_parent(c) != cur) {
				break;
			}
			cur = c;
		}

		int L = list_size(chain);
		if (L < 2) {
			list_delete(chain);
			continue;
		}

		struct pending_chain_group *pc = xxmalloc(sizeof(*pc));
		pc->leader_id = n->node_id;
		pc->n_members = L - 1;
		pc->member_ids = xxmalloc((size_t)(L - 1) * sizeof(uint64_t));
		int mi = 0;
		int first = 1;
		struct node *x;
		LIST_ITERATE(chain, x)
		{
			if (first) {
				first = 0;
				continue;
			}
			pc->member_ids[mi++] = x->node_id;
		}
		list_push_tail(pending, pc);
		list_delete(chain);
	}

	int groups = 0;
	struct pending_chain_group *pc;
	while ((pc = (struct pending_chain_group *)list_pop_head(pending))) {
		if (graph_supernode_register(g, pc->leader_id, pc->member_ids, pc->n_members) == 0) {
			groups++;
		}
		free(pc->member_ids);
		free(pc);
	}
	list_delete(pending);

	return groups;
}

struct node *graph_input_producer_node(struct graph *g, struct node *parent, struct node *child)
{
	if (!g || !parent || !child) {
		return parent;
	}
	if (!g->chain_grouping_enabled || !g->supernode_leader_child_to_input_source) {
		return parent;
	}

	char *k = string_format("%" PRIu64 ",%" PRIu64, parent->node_id, child->node_id);
	struct node *src = (struct node *)hash_table_lookup(g->supernode_leader_child_to_input_source, k);
	free(k);
	return src ? src : parent;
}

/**
 * Delete an executor graph instance.
 * @param g Reference to the executor graph.
 */
void graph_delete(struct graph *g)
{
	if (!g) {
		return;
	}

	uint64_t lid;
	struct list *mems;
	if (g->super_leader_to_members) {
		ITABLE_ITERATE(g->super_leader_to_members, lid, mems)
		{
			(void)lid;
			if (mems) {
				list_delete(mems);
			}
		}
		itable_delete(g->super_leader_to_members);
		g->super_leader_to_members = NULL;
	}

	uint64_t nid;
	struct node *node;
	ITABLE_ITERATE(g->nodes, nid, node)
	{
		node_delete(node);
	}

	free(g->task_runner_library_name);
	free(g->task_runner_function_name);
	free(g->checkpoint_dir);
	free(g->output_dir);

	itable_delete(g->nodes);
	hash_table_delete(g->outfile_cachename_to_node);

	hash_table_delete(g->supernode_leader_child_to_input_source);

	hash_table_clear(g->inout_filename_to_cached_name, (void *)free);
	hash_table_delete(g->inout_filename_to_cached_name);

	free(g);
}
