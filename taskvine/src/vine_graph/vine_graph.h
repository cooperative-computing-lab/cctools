#ifndef VINE_GRAPH_H
#define VINE_GRAPH_H

#include "hash_table.h"
#include "itable.h"

#include "vine_graph_node.h"

struct vine_graph {
	struct itable *nodes;
	/** Maps each supernode leader id to the list of non-leader member nodes. Empty when no groups exist. */
	struct itable *super_leader_to_members;
	struct hash_table *outfile_cachename_to_node;
	struct hash_table *inout_filename_to_cached_name;
	/**
	 * Maps leader id and downstream child id to the member that actually wrote the outfile the
	 * child should read. After a rewire the DAG edge may show the leader while the tail member
	 * still owns the bytes. Keys are decimal node ids joined with a comma.
	 */
	struct hash_table *supernode_leader_child_to_input_source;

	char *checkpoint_dir;
	char *output_dir;
	char *task_runner_library_name;
	char *task_runner_function_name;

	double checkpoint_fraction;
	int prune_depth;

	int print_graph_details;
	/*
	 * Zero unless the user turned on chain grouping. When zero the executor does not merge
	 * members onto one task, does not list several scheduler keys in one runner infile, and does
	 * not remap which node's outfile a consumer should mount.
	 */
	int chain_grouping_enabled;
};

// Public graph API (declarations below)

/**
 * Resolve which producer node's outfile a child should consume for a scheduling parent edge.
 * After supernode rewire, parent may be the leader while the file is produced by a member (e.g. tail).
 * @return The node that owns the vine_file inputs should mount, or @p parent if there is no override.
 */
struct vine_graph_node *vine_graph_input_producer_node(struct vine_graph *g, struct vine_graph_node *parent, struct vine_graph_node *child);

/** Create an executor graph and return it.
@param runtime_dir Runtime directory used for default graph output paths.
@return A new executor graph.
*/
struct vine_graph *vine_graph_create(const char *runtime_dir);

/** Create a new node in the executor graph.
@param g Reference to the executor graph.
@return The auto-assigned node id.
*/
uint64_t vine_graph_add_node(struct vine_graph *g);

/** Mark a node as a retrieval target.
@param g Reference to the executor graph.
@param node_id Identifier of the node to mark as target.
*/
void vine_graph_set_target(struct vine_graph *g, uint64_t node_id);

/** Add a dependency between two nodes in the executor graph.
@param g Reference to the executor graph.
@param parent_id Identifier of the parent node.
@param child_id Identifier of the child node.
*/
void vine_graph_add_dependency(struct vine_graph *g, uint64_t parent_id, uint64_t child_id);

/** Finalize the metrics of the executor graph.
@param g Reference to the executor graph.
*/
void vine_graph_finalize(struct vine_graph *g);

/** Get the heavy score of a node in the executor graph.
@param g Reference to the executor graph.
@param node_id Identifier of the node.
@return The heavy score.
*/
double vine_graph_get_node_heavy_score(const struct vine_graph *g, uint64_t node_id);

/** Get the outfile remote name of a node in the executor graph.
@param g Reference to the executor graph.
@param node_id Identifier of the node.
@return The outfile remote name.
*/
const char *vine_graph_get_node_outfile_remote_name(const struct vine_graph *g, uint64_t node_id);

/** Delete an executor graph.
@param g Reference to the executor graph.
*/
void vine_graph_delete(struct vine_graph *g);

/** Get the task runner library name of the executor graph.
@param g Reference to the executor graph.
@return The task runner library name.
*/
const char *vine_graph_get_task_runner_library_name(const struct vine_graph *g);

/** Set the task runner function name of the executor graph.
@param g Reference to the executor graph.
@param task_runner_function_name Reference to the task runner function name.
*/
void vine_graph_set_task_runner_function_name(struct vine_graph *g, const char *task_runner_function_name);

/** Tune the executor graph.
@param g Reference to the executor graph.
@param name Reference to the name of the parameter to tune.
@param value Reference to the value of the parameter to tune.
@return 0 on success, -1 on failure.
*/
int vine_graph_tune(struct vine_graph *g, const char *name, const char *value);

/**
 * True if this node may submit a TaskVine task: singletons and supernode leaders only.
 * Non-leader members run inside the leader's runner and must not be scheduled separately.
 * Returns non-zero when n is non-NULL and node_id equals super_leader_id.
 */
static inline int vine_graph_node_is_supernode_leader(const struct vine_graph_node *n)
{
	return n && n->node_id == n->super_leader_id;
}

/**
 * Look up the leader node for n using n->super_leader_id (identity when n is already the leader).
 * Returns the leader struct vine_graph_node *, or NULL on invalid input or if the leader id is missing from g.
 */
struct vine_graph_node *vine_graph_supernode_leader_node(struct vine_graph *g, struct vine_graph_node *n);

/**
 * Merge nodes into one supernode: rewire external edges to leader_id and set super_leader_id on all members.
 * Call after all plain vine_graph_add_dependency edges exist and before vine_graph_executor_finalize.
 * Every involved node must currently be its own group (super_leader_id == node_id); member_ids must list
 * only non-leader members (not leader_id) without duplicates. Returns 0 on success, -1 on error.
 */
int vine_graph_supernode_register(struct vine_graph *g, uint64_t leader_id, const uint64_t *member_ids, int n_member_ids);

/**
 * Non-leader members for a registered supernode as a list of struct vine_graph_node *.
 * Do not free the list; it is owned by g. Returns NULL if g is NULL or no group was registered for that leader.
 */
struct list *vine_graph_supernode_nonleader_members(struct vine_graph *g, uint64_t leader_id);

/**
 * Collapse each maximal singleton linear chain into one supernode (leader = chain head).
 * A chain is a path n0->n1->... where each n_i (i > 0) has exactly one parent (n_{i-1}) and
 * each n_i (i < last) has exactly one child (n_{i+1}); n0 is not preceded by such an edge from a
 * single-child parent (head: no parent, multiple parents, or parent with multiple children).
 * Call after all vine_graph_add_dependency edges and before vine_graph_executor_finalize.
 * Returns the number of supernodes registered, or -1 if g is NULL.
 */
int vine_graph_group_chain_like_tasks(struct vine_graph *g);

#endif // VINE_GRAPH_H
