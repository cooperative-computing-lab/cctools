#include <inttypes.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "debug.h"
#include "executor.h"
#include "macros.h"
#include "progress_bar.h"
#include "random.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "vine_file.h"
#include "vine_mount.h"
#include "vine_task.h"
#include "vine_temp.h"

static volatile sig_atomic_t interrupted = 0;

static void submit_node_task(struct executor *e, struct node *node);
static struct vine_task *executor_make_vine_task(struct executor *e);
static void executor_materialize_node(struct executor *e, struct node *node);

static void extra_io_mount_push(struct list *lst, struct vine_file *f, const char *remote_name)
{
	struct extra_io_mount *m = xxmalloc(sizeof(*m));
	m->file = f;
	m->remote_name = xxstrdup(remote_name);
	list_push_tail(lst, m);
}

/* Undeclare runner infile buffer (before discarding the vine_task). */
static void executor_clear_node_runner_arg(struct executor *e, struct node *node)
{
	if (!e || !node || !node->task_runner_arg_file) {
		return;
	}
	vine_undeclare_file(e->manager, node->task_runner_arg_file);
	node->task_runner_arg_file = NULL;
}

/* Initialize runtime fields and default tuning values for a new executor. */
static void executor_init_runtime(struct executor *e)
{
	if (!e) {
		return;
	}

	e->task_id_to_node = itable_create(0);
	e->resubmit_queue = list_create();
	e->time_first_task_dispatched = UINT64_MAX; // sentinel until first task commit time
	e->time_last_task_retrieved = 0;
	e->makespan_us = 0;
	e->completed_recovery_tasks = 0;
	e->time_spent_on_cut_propagation = 0;
	e->pfs_usage_bytes = 0;
	e->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST;
	e->failure_injection_step_percent = -1.0;
	e->progress_bar_update_interval_sec = 0.1;
	e->enable_debug_log = 1;
}

/* Release the task-id lookup table and the resubmit queue. */
static void executor_clear_runtime(struct executor *e)
{
	if (!e) {
		return;
	}
	if (e->task_id_to_node) {
		itable_delete(e->task_id_to_node);
		e->task_id_to_node = NULL;
	}
	if (e->resubmit_queue) {
		list_delete(e->resubmit_queue);
		e->resubmit_queue = NULL;
	}
}

/* Allocate an executor bound to the given manager and graph. */
struct executor *executor_create(struct vine_manager *manager, struct graph *graph)
{
	if (!manager || !graph) {
		return NULL;
	}

	struct executor *e = malloc(sizeof(*e));
	if (!e) {
		return NULL;
	}

	e->graph = graph;
	e->manager = manager;
	executor_init_runtime(e);
	return e;
}

/* Create a new graph for the manager and enable debug logging under the runtime directory. */
struct graph *executor_graph_create(struct vine_manager *manager)
{
	if (!manager) {
		return NULL;
	}

	char *debug_tmp = string_format("%s/vine-logs/debug", manager->runtime_directory);
	vine_enable_debug_log(debug_tmp);
	free(debug_tmp);

	return graph_create(manager->runtime_directory);
}

/* Undeclare managed files, remove local outputs, and free the executor. */
void executor_delete(struct executor *e)
{
	struct graph *g = e ? e->graph : NULL;
	if (g && e->manager) {
		uint64_t nid;
		struct node *node;
		ITABLE_ITERATE(g->nodes, nid, node)
		{
			if (node->task_runner_arg_file) {
				vine_undeclare_file(e->manager, node->task_runner_arg_file); // before graph free to avoid double free
				node->task_runner_arg_file = NULL;
			}
			switch (node->outfile_type) {
			case NODE_OUTFILE_TYPE_TEMP:
				break;
			case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
				if (node->outfile_remote_name) {
					unlink(node->outfile_remote_name);
				}
				break;
			case NODE_OUTFILE_TYPE_LOCAL:
				if (node->outfile && node->outfile->source) {
					unlink(node->outfile->source);
				}
				break;
			}
			if (node->outfile) {
				hash_table_remove(g->outfile_cachename_to_node, node->outfile->cached_name);
				vine_undeclare_file(e->manager, node->outfile);
				node->outfile = NULL;
			}
		}
	}
	executor_clear_runtime(e);
	free(e);
}

/*
 * Create a new library task (not yet published on the node). The caller attaches IO, then sets
 * node->task only when the task is fully configured (atomic materialize).
 */
static struct vine_task *executor_make_vine_task(struct executor *e)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g) {
		return NULL;
	}

	if (!g->task_runner_function_name) {
		debug(D_ERROR, "task runner function name is not set");
		graph_delete(g);
		exit(1);
	}
	if (!g->task_runner_library_name) {
		debug(D_ERROR, "task runner library name is not set");
		graph_delete(g);
		exit(1);
	}

	struct vine_task *t = vine_task_create(g->task_runner_function_name);
	vine_task_set_library_required(t, g->task_runner_library_name);
	vine_task_addref(t); // keep alive across vine_submit and vine_wait
	return t;
}

/*
 * Runtime materialization (submit path): logical graph and vine_file handles are fixed in
 * executor_finalize. Here we build vine_task + mounts + runner infile as one atomic step:
 * node->task is non-NULL only after all IO and the infile are attached. On failure the ephemeral
 * task is discarded and node->task stays NULL.
 */
static void executor_materialize_node(struct executor *e, struct node *node)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}

	if (node->task && node->task->state != VINE_TASK_INITIAL) {
		return;
	}
	/* INITIAL task implies mounts + runner infile are already complete. */
	if (node->task) {
		return;
	}

	executor_clear_node_runner_arg(e, node);

	struct vine_task *t = executor_make_vine_task(e);
	if (!t) {
		return;
	}

	if (node->outfile) {
		vine_task_add_output(t, node->outfile, node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
	}

	void *item;
	LIST_ITERATE(node->extra_outputs, item)
	{
		struct extra_io_mount *m = (struct extra_io_mount *)item;
		vine_task_add_output(t, m->file, m->remote_name, VINE_TRANSFER_ALWAYS);
	}

	struct node *parent_node;
	LIST_ITERATE(node->parents, parent_node)
	{
		if (parent_node->outfile) {
			vine_task_add_input(t, parent_node->outfile, parent_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
		}
	}

	LIST_ITERATE(node->extra_inputs, item)
	{
		struct extra_io_mount *m = (struct extra_io_mount *)item;
		vine_task_add_input(t, m->file, m->remote_name, VINE_TRANSFER_ALWAYS);
	}

	char *task_arguments = node_construct_task_arguments(node);
	if (!task_arguments) {
		goto fail_task;
	}
	struct vine_file *arg_file =
			vine_declare_buffer(e->manager, task_arguments, strlen(task_arguments), VINE_CACHE_LEVEL_TASK, VINE_UNLINK_WHEN_DONE);
	free(task_arguments);
	if (!arg_file) {
		goto fail_task;
	}
	vine_task_add_input(t, arg_file, "infile", VINE_TRANSFER_ALWAYS);

	node->task = t;
	node->task_runner_arg_file = arg_file;
	return;

fail_task:
	vine_task_delete(t);
}

/*
 * Declare the output vine_file for this node from outfile_type.
 * Shared filesystem outputs may leave outfile unset.
 */
static void executor_declare_node_outfile(struct executor *e, struct node *node)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !node || node->outfile) {
		return;
	}

	switch (node->outfile_type) {
	case NODE_OUTFILE_TYPE_LOCAL: {
		char *local_outfile_path = string_format("%s/%s", g->output_dir, node->outfile_remote_name);
		node->outfile = vine_declare_file(e->manager, local_outfile_path, VINE_CACHE_LEVEL_WORKFLOW, 0);
		free(local_outfile_path);
		break;
	}
	case NODE_OUTFILE_TYPE_TEMP:
		node->outfile = vine_declare_temp(e->manager);
		break;
	case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
		break;
	}
}

/* Add a graph node; vine_task and IO mounts are created at submit (materialize). */
uint64_t executor_add_node(struct executor *e)
{
	if (!e || !e->graph) {
		return 0;
	}

	uint64_t node_id = graph_add_node(e->graph);
	return node_id;
}

/*
 * Finalize the graph: declare outputs with cached_name registration,
 * attach parent inputs, and set remaining parent counts for scheduling.
 */
void executor_finalize(struct executor *e)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g) {
		return;
	}

	graph_finalize(g);

	/*
	 * Two passes. Declare outputs and cached_name map first so parent
	 * vine_file objects exist. Task-level input/output mounts are applied
	 * in executor_materialize_node at submit time.
	 */
	uint64_t nid;
	struct node *node;
	ITABLE_ITERATE(g->nodes, nid, node)
	{
		executor_declare_node_outfile(e, node);
		if (node->outfile) {
			hash_table_insert(g->outfile_cachename_to_node, node->outfile->cached_name, node);
		}
	}

	ITABLE_ITERATE(g->nodes, nid, node)
	{
		node->remaining_parents_count = list_size(node->parents);
	}
}

/* Add a named input, reusing a declared file when the logical filename was already mapped. */
void executor_add_task_input(struct executor *e, uint64_t task_id, const char *filename)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !task_id || !filename) {
		return;
	}

	struct node *node = itable_lookup(g->nodes, task_id);
	if (!node) {
		return;
	}

	struct vine_file *f = NULL;
	const char *cached_name = hash_table_lookup(g->inout_filename_to_cached_name, filename);

	if (cached_name) {
		f = vine_manager_lookup_file(e->manager, cached_name);
	} else {
		f = vine_declare_temp(e->manager); // first use of logical name, record cache key for paired mounts
		hash_table_insert(g->inout_filename_to_cached_name, filename, xxstrdup(f->cached_name));
	}

	extra_io_mount_push(node->extra_inputs, f, filename);
}

/* Add a named output, linking logical filename to a cache name for paired producer and consumer tasks. */
void executor_add_task_output(struct executor *e, uint64_t task_id, const char *filename)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !task_id || !filename) {
		return;
	}

	struct node *node = itable_lookup(g->nodes, task_id);
	if (!node) {
		return;
	}

	struct vine_file *f = NULL;
	const char *cached_name = hash_table_lookup(g->inout_filename_to_cached_name, filename);

	if (cached_name) {
		f = vine_manager_lookup_file(e->manager, cached_name);
	} else {
		f = vine_declare_temp(e->manager); // pair with matching input on consumer tasks
		hash_table_insert(g->inout_filename_to_cached_name, filename, xxstrdup(f->cached_name));
	}

	extra_io_mount_push(node->extra_outputs, f, filename);
}

/* Apply executor-level tuning. Unknown keys are forwarded to graph_tune. */
int executor_tune(struct executor *e, const char *name, const char *value)
{
	if (!e || !name || !value) {
		return -1;
	}

	if (strcmp(name, "failure-injection-step-percent") == 0) {
		e->failure_injection_step_percent = atof(value);

	} else if (strcmp(name, "task-priority-mode") == 0) {
		if (strcmp(value, "random") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_RANDOM;
		} else if (strcmp(value, "depth-first") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_DEPTH_FIRST;
		} else if (strcmp(value, "breadth-first") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_BREADTH_FIRST;
		} else if (strcmp(value, "fifo") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_FIFO;
		} else if (strcmp(value, "lifo") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_LIFO;
		} else if (strcmp(value, "largest-input-first") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST;
		} else if (strcmp(value, "largest-storage-footprint-first") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST;
		} else {
			debug(D_ERROR, "invalid priority mode: %s", value);
			return -1;
		}

	} else if (strcmp(name, "progress-bar-update-interval-sec") == 0) {
		double val = atof(value);
		e->progress_bar_update_interval_sec = (val > 0.0) ? val : 0.1;

	} else if (strcmp(name, "enable-debug-log") == 0) {
		if (e->enable_debug_log == 0) {
			return -1;
		}
		e->enable_debug_log = (atoi(value) == 1) ? 1 : 0;
		if (e->enable_debug_log == 0) {
			debug_flags_clear();
			debug_close();
		}

	} else {
		return graph_tune(e->graph, name, value);
	}

	return 0;
}

/* Set the interrupted flag when SIGINT is received. */
static void handle_sigint(int signal)
{
	interrupted = 1;
}

/* Compute submission priority for a node using the configured scheduling policy. */
static double calculate_task_priority(struct node *node, task_priority_mode_t priority_mode)
{
	if (!node) {
		return 0;
	}

	double priority = 0;
	timestamp_t current_time = timestamp_get();
	struct node *parent_node;

	switch (priority_mode) {
	case TASK_PRIORITY_MODE_RANDOM:
		priority = random_double();
		break;
	case TASK_PRIORITY_MODE_DEPTH_FIRST:
		priority = (double)node->depth;
		break;
	case TASK_PRIORITY_MODE_BREADTH_FIRST:
		priority = -(double)node->depth;
		break;
	case TASK_PRIORITY_MODE_FIFO:
		priority = -(double)current_time; // earlier time yields higher priority
		break;
	case TASK_PRIORITY_MODE_LIFO:
		priority = (double)current_time;
		break;
	case TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST:
		LIST_ITERATE(node->parents, parent_node)
		{
			if (!parent_node->outfile) {
				continue;
			}
			priority += (double)vine_file_size(parent_node->outfile);
		}
		break;
	case TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST:
		LIST_ITERATE(node->parents, parent_node)
		{
			if (!parent_node->outfile) {
				continue;
			}
			if (!parent_node->task) {
				continue;
			}
			timestamp_t parent_task_completion_time = parent_node->task->time_workers_execute_last;
			// weight by parent input size and last observed worker runtime
			priority += (double)vine_file_size(parent_node->outfile) * (double)parent_task_completion_time;
		}
		break;
	}

	return priority;
}

/* Submit the node task if it is still initial, and record the manager task id for later lookup. */
static void submit_node_task(struct executor *e, struct node *node)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}

	executor_materialize_node(e, node);

	if (!node->task) {
		debug(D_ERROR, "submit_node_task: node %" PRIu64 " has no task after materialize", node->node_id);
		return;
	}

	if (node->task->state != VINE_TASK_INITIAL) {
		debug(D_VINE, "submit_node_task: skipping node %" PRIu64 " (task already submitted, state=%d, task_id=%d)", node->node_id, node->task->state, node->task->task_id);
		return; // not INITIAL, manager already owns this submission
	}

	double priority = calculate_task_priority(node, e->task_priority_mode);
	vine_task_set_priority(node->task, priority);

	int task_id = vine_submit(e->manager, node->task);

	if (task_id <= 0) {
		debug(D_ERROR, "submit_node_task: failed to submit node %" PRIu64 " (returned task_id=%d)", node->node_id, task_id);
		return;
	}

	itable_insert(e->task_id_to_node, (uint64_t)task_id, node); // reverse lookup from vine_wait
	debug(D_VINE, "submitted node %" PRIu64 " with task id %d", node->node_id, task_id);
}

static int node_ready_for_submission(struct executor *e, struct node *node)
{
	if (!e || !node || node->remaining_parents_count != 0 || node->completed) {
		return 0;
	}
	if (node->in_resubmit_queue) {
		return 0;
	}
	if (node->task && node->task->state != VINE_TASK_INITIAL) {
		return 0;
	}
	return 1;
}

/* Submit ready source nodes and enable delivery of recovery tasks to the application. */
static void submit_initial_ready_nodes(struct executor *e)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !e->manager) {
		return;
	}

	uint64_t nid;
	struct node *node;
	ITABLE_ITERATE(g->nodes, nid, node)
	{
		if (node->remaining_parents_count == 0) {
			submit_node_task(e, node);
		}
	}

	vine_enable_return_recovery_tasks(e->manager); // driver must observe recovery completions for cut and prune
}

/* After one parent completes, decrement remaining parents and submit children that become ready. */
static void submit_unblocked_children(struct executor *e, struct node *node)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}

	struct node *child_node;
	LIST_ITERATE(node->children, child_node)
	{
		if (!child_node) {
			continue;
		}

		if (!child_node->fired_parents) {
			child_node->fired_parents = set_create(0);
		}
		if (set_lookup(child_node->fired_parents, node)) {
			continue;
		}
		set_insert(child_node->fired_parents, node);

		if (child_node->remaining_parents_count > 0) {
			child_node->remaining_parents_count--;
		}

		if (node_ready_for_submission(e, child_node)) {
			submit_node_task(e, child_node);
		}
	}
}

/* Map a completed vine_task to the corresponding graph node, including recovery tasks. */
static struct node *get_node_by_task(struct executor *e, struct vine_task *task)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !task) {
		return NULL;
	}

	if (task->type == VINE_TASK_TYPE_STANDARD) {
		return itable_lookup(e->task_id_to_node, (uint64_t)task->task_id);
	} else if (task->type == VINE_TASK_TYPE_RECOVERY) {
		/*
		 * Recovery tasks are not stored in task_id_to_node.
		 * Resolve the logical node via the original producer id on output mounts.
		 */
		struct vine_mount *mount;
		LIST_ITERATE(task->output_mounts, mount)
		{
			if (mount->file && mount->file->cached_name) {
				struct node *producer = hash_table_lookup(g->outfile_cachename_to_node, mount->file->cached_name);
				if (producer) {
					return producer;
				}
			}
			if (!mount->file) {
				continue;
			}
			uint64_t original_producer_task_id = mount->file->original_producer_task_id;
			if (original_producer_task_id > 0) {
				return itable_lookup(e->task_id_to_node, original_producer_task_id);
			}
		}
	}

	debug(D_ERROR, "task %d has no original producer task id", task->task_id);
	return NULL;
}

/* Update shared-filesystem byte counters when a node's credited output size changes. */
static void pfs_account_write(struct executor *e, struct node *n, size_t new_size)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !n) {
		return;
	}

	size_t prev = n->pfs_credited_bytes;
	if (new_size == prev) {
		return;
	}

	if (new_size > prev) {
		e->pfs_usage_bytes += (new_size - prev);
	} else {
		e->pfs_usage_bytes -= (prev - new_size); // retry may produce a smaller file
	}
	n->pfs_credited_bytes = new_size;

	debug(D_VINE,
			"pfs write: node %" PRIu64 " size=%zu (prev=%zu) usage=%" PRIu64,
			n->node_id,
			new_size,
			prev,
			e->pfs_usage_bytes);
}

/* Remove a node's credited bytes from the shared-filesystem usage total. */
static void pfs_account_delete(struct executor *e, struct node *n)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !n) {
		return;
	}

	size_t credited = n->pfs_credited_bytes;
	if (credited == 0) {
		return;
	}

	e->pfs_usage_bytes -= credited;
	n->pfs_credited_bytes = 0;

	debug(D_VINE,
			"pfs delete: node %" PRIu64 " size=%zu usage=%" PRIu64,
			n->node_id,
			credited,
			e->pfs_usage_bytes);
}

/* Return non-zero if the completed node retains output on local disk or a shared filesystem path. */
static int node_is_anchored(const struct node *n)
{
	if (!n || !n->completed) {
		return 0;
	}
	return n->outfile_type == NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM || n->outfile_type == NODE_OUTFILE_TYPE_LOCAL;
}

/* Return non-zero when a temporary output file has a recovery task that is neither initial nor finished. */
static int node_is_mid_recovery(const struct node *n)
{
	if (!n || !n->outfile || n->outfile->type != VINE_TEMP) {
		return 0;
	}
	struct vine_task *rt = n->outfile->recovery_task;
	if (!rt) {
		return 0;
	}
	return rt->state != VINE_TASK_INITIAL && rt->state != VINE_TASK_DONE; // recovery in flight
}

/* Remove or prune the node's result file according to its output storage mode. */
static void delete_node_return_file(struct executor *e, struct node *n)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !n) {
		return;
	}

	switch (n->outfile_type) {
	case NODE_OUTFILE_TYPE_TEMP:
		if (n->outfile) {
			vine_prune_file(e->manager, n->outfile);
		}
		break;
	case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
		pfs_account_delete(e, n);
		if (n->outfile_remote_name) {
			unlink(n->outfile_remote_name);
		}
		break;
	case NODE_OUTFILE_TYPE_LOCAL:
		if (n->outfile && n->outfile->source) {
			unlink(n->outfile->source);
		}
		break;
	}
}

/* Attempt to mark a completed node as cut and delete its return file when all children permit release. */
static int try_cut_node(struct executor *e, struct node *n)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !n || n->cut || !n->completed) {
		return 0;
	}

	struct node *c;
	LIST_ITERATE(n->children, c)
	{
		if ((!node_is_anchored(c) && !c->cut) || node_is_mid_recovery(c)) {
			return 0; // wait for anchored, cut, or non-recovery children
		}
	}

	n->cut = 1;
	debug(D_VINE, "cut: node %" PRIu64 " outfile_type=%d is_target=%d", n->node_id, n->outfile_type, n->is_target);

	if (!n->is_target) {
		delete_node_return_file(e, n); // targets keep data for retrieval
	}

	return 1;
}

/* Walk upstream from a completed node and apply cut propagation along the worklist. */
static void propagate_cut_from(struct executor *e, struct node *start)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !start || !start->completed) {
		return;
	}

	timestamp_t t0 = timestamp_get();
	try_cut_node(e, start);

	/* Upstream BFS: when a node is cut, enqueue its parents for the same check. */
	struct list *worklist = list_create();
	struct node *p;
	LIST_ITERATE(start->parents, p)
	{
		list_push_tail(worklist, p);
	}

	while (list_size(worklist) > 0) {
		struct node *m = list_pop_head(worklist);
		if (try_cut_node(e, m)) {
			LIST_ITERATE(m->parents, p)
			{
				list_push_tail(worklist, p);
			}
		}
	}

	list_delete(worklist);
	e->time_spent_on_cut_propagation += timestamp_get() - t0;
}

/* Return non-zero if every descendant within the given depth bound is complete and not mid-recovery. */
static int all_descendants_within_depth_completed(struct node *a, int depth)
{
	if (!a || depth <= 0) {
		return 1;
	}

	struct set *visited = set_create(0);
	struct list *current = list_create();
	list_push_tail(current, a);
	set_insert(visited, a);

	int ok = 1;
	/* Expand one child frontier per iteration up to depth hops from a. */
	for (int d = 0; d < depth && ok; d++) {
		struct list *next = list_create();
		struct node *n;
		LIST_ITERATE(current, n)
		{
			struct node *c;
			LIST_ITERATE(n->children, c)
			{
				if (set_lookup(visited, c)) {
					continue;
				}
				set_insert(visited, c);
				if (!c->completed || node_is_mid_recovery(c)) {
					ok = 0;
					break;
				}
				list_push_tail(next, c);
			}
			if (!ok) {
				break;
			}
		}
		list_delete(current);
		current = next;
	}

	list_delete(current);
	set_delete(visited);
	return ok;
}

/* Release a temporary output when prune-depth constraints and descendant completion are satisfied. */
static void try_prune_depth_release(struct executor *e, struct node *a)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !a) {
		return;
	}
	if (a->prune_depth_pruned) {
		return;
	}
	if (a->outfile_type != NODE_OUTFILE_TYPE_TEMP) {
		return;
	}
	if (a->is_target) {
		return;
	}
	if (!a->completed || !a->outfile) {
		return;
	}
	if (!all_descendants_within_depth_completed(a, g->prune_depth)) {
		return; // wait until descendants within prune_depth layers are settled
	}

	delete_node_return_file(e, a);
	a->prune_depth_pruned = 1;

	debug(D_VINE, "prune-depth release: node %" PRIu64 " depth=%d", a->node_id, g->prune_depth);
}

/* Apply prune-depth release starting at a node and extending up to k ancestor levels. */
static void apply_prune_depth_from(struct executor *e, struct node *node)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}
	int k = g->prune_depth;
	if (k <= 0) {
		return;
	}

	try_prune_depth_release(e, node);

	struct set *visited = set_create(0);
	struct list *current = list_create();
	list_push_tail(current, node);
	set_insert(visited, node);

	/* Visit new parents up to k levels, trying prune release on each. */
	for (int d = 1; d <= k; d++) {
		struct list *next = list_create();
		struct node *n;
		LIST_ITERATE(current, n)
		{
			struct node *p;
			LIST_ITERATE(n->parents, p)
			{
				if (set_lookup(visited, p)) {
					continue;
				}
				set_insert(visited, p);
				list_push_tail(next, p);
				try_prune_depth_release(e, p);
			}
		}
		list_delete(current);
		current = next;
	}

	list_delete(current);
	set_delete(visited);
}

#define RESUBMIT_SCAN_LIMIT 100
#define RESUBMIT_COOLDOWN_USECS ((timestamp_t)1000000)

/* Append a failed node to the resubmit queue after recording the failure time. */
static void queue_node_for_retry(struct executor *e, struct node *node)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}

	if (node->in_resubmit_queue) {
		return;
	}

	node->last_failure_time = timestamp_get();
	list_push_tail(e->resubmit_queue, node);
	node->in_resubmit_queue = 1;
}

/* Dequeue nodes whose cooldown has expired; discard the failed task atomically and resubmit. */
static void drain_resubmit_queue(struct executor *e)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g) {
		return;
	}

	timestamp_t now = timestamp_get();
	int queued = list_size(e->resubmit_queue);
	int budget = queued < RESUBMIT_SCAN_LIMIT ? queued : RESUBMIT_SCAN_LIMIT;

	// drain at most budget entries, stop early if head remains in cooldown
	for (int i = 0; i < budget; i++) {
		struct node *node = list_peek_head(e->resubmit_queue);
		if (!node) {
			break;
		}
		if (now - node->last_failure_time < RESUBMIT_COOLDOWN_USECS) {
			break;
		}

		list_pop_head(e->resubmit_queue);
		node->in_resubmit_queue = 0;

		debug(D_VINE, "Resubmitting node %" PRIu64, node->node_id);
		executor_clear_node_runner_arg(e, node);
		if (node->task) {
			vine_task_delete(node->task);
			node->task = NULL;
		}
		submit_node_task(e, node);
	}
}

/*
 * Verify status and on-disk outputs.
 * On failure enqueue a retry for the node.
 */
static int validate_node_outputs_or_enqueue(struct executor *e, struct node *retry_node, struct node *output_node, struct vine_task *task)
{
	switch (output_node->outfile_type) {
	case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM: {
		struct stat info;
		// shared path may lag behind a successful task result code
		if (stat(output_node->outfile_remote_name, &info) < 0) {
			debug(D_VINE, "Task %d succeeded but missing sharedfs output %s", task->task_id, output_node->outfile_remote_name);
			queue_node_for_retry(e, retry_node);
			return 0;
		}
		output_node->outfile_size_bytes = info.st_size;
		pfs_account_write(e, output_node, (size_t)info.st_size);
		break;
	}
	case NODE_OUTFILE_TYPE_LOCAL:
	case NODE_OUTFILE_TYPE_TEMP:
		if (output_node->outfile) {
			output_node->outfile_size_bytes = output_node->outfile->size;
		}
		break;
	}

	return 1;
}

static int validate_task_or_enqueue(struct executor *e, struct node *node, struct vine_task *task)
{
	if (task->result != VINE_RESULT_SUCCESS || task->exit_code != 0) {
		debug(D_VINE, "Task %d failed (result=%d, exit=%d)", task->task_id, task->result, task->exit_code);
		queue_node_for_retry(e, node);
		return 0;
	}

	return validate_node_outputs_or_enqueue(e, node, node, task);
}

/* Return the recorded user-task makespan in microseconds. */
uint64_t executor_get_makespan_us(const struct executor *e)
{
	if (!e) {
		return 0;
	}

	return (uint64_t)e->makespan_us;
}

/* Return the manager's cumulative count of recovery tasks submitted. */
uint64_t executor_get_total_recovery_tasks(const struct executor *e)
{
	if (!e || !e->manager || !e->manager->stats) {
		return 0;
	}

	return (uint64_t)e->manager->stats->tasks_recovery;
}

/* Return how many recovery tasks have completed in the current executor run. */
uint64_t executor_get_completed_recovery_tasks(const struct executor *e)
{
	if (!e) {
		return 0;
	}

	return e->completed_recovery_tasks;
}

/* Main loop: submit work, wait, handle recovery, update graph state. */
void executor_execute(struct executor *e)
{
	struct graph *g = e ? e->graph : NULL;
	if (!g) {
		return;
	}

	void (*previous_sigint_handler)(int) = signal(SIGINT, handle_sigint);

	debug(D_VINE, "start executing executor graph");

	submit_initial_ready_nodes(e);

	struct ProgressBar *pbar = progress_bar_init("Executing Tasks");
	progress_bar_set_update_interval(pbar, e->progress_bar_update_interval_sec);
	e->completed_recovery_tasks = 0;

	struct ProgressBarPart *user_tasks_part = progress_bar_create_part("User", itable_size(g->nodes));
	struct ProgressBarPart *recovery_tasks_part = progress_bar_create_part("Recovery", 0);
	progress_bar_bind_part(pbar, user_tasks_part);
	progress_bar_bind_part(pbar, recovery_tasks_part);

	double next_failure_threshold = -1.0;
	if (e->failure_injection_step_percent > 0) {
		next_failure_threshold = e->failure_injection_step_percent / 100.0;
	}

	int wait_timeout = 1; // short timeout after a result, longer when idle

	/*
	 * Finish when every user node has succeeded at least once.
	 * Recovery tasks do not advance user_tasks_part.
	 */
	while (user_tasks_part->current < user_tasks_part->total) {
		if (interrupted) {
			break;
		}

		drain_resubmit_queue(e);
		progress_bar_set_part_total(pbar, recovery_tasks_part, executor_get_total_recovery_tasks(e));

		struct vine_task *task = vine_wait(e->manager, wait_timeout);
		if (task) {
			wait_timeout = 0;

			struct node *node = get_node_by_task(e, task);
			if (!node) {
				debug(D_ERROR, "fatal: task %d could not be mapped to a task node, this indicates a serious bug.", task->task_id);
				exit(1);
			}

			if (task->time_when_commit_end > 0) {
				e->time_first_task_dispatched = MIN(e->time_first_task_dispatched, task->time_when_commit_end); // makespan start
			}

			if (task->type == VINE_TASK_TYPE_RECOVERY) {
				e->completed_recovery_tasks++;
				progress_bar_update_part(
						pbar,
						recovery_tasks_part,
						e->completed_recovery_tasks - recovery_tasks_part->current);

				if (!validate_task_or_enqueue(e, node, task)) {
					continue;
				}

				node->cut = 0;
				node->prune_depth_pruned = 0;
				// recovery restored output, re-run release logic for this subgraph
				propagate_cut_from(e, node);
				apply_prune_depth_from(e, node);
				continue;
			}

			if (!validate_task_or_enqueue(e, node, task)) {
				continue;
			}

			e->time_last_task_retrieved = MAX(e->time_last_task_retrieved, task->time_when_retrieval);
			if (e->time_last_task_retrieved < e->time_first_task_dispatched) {
				debug(D_ERROR,
						"task %d time_last_task_retrieved < time_first_task_dispatched: %" PRIu64 " < %" PRIu64,
						task->task_id,
						e->time_last_task_retrieved,
						e->time_first_task_dispatched);
				e->time_last_task_retrieved = e->time_first_task_dispatched; // clamp non-monotonic timestamps
			}
			e->makespan_us = e->time_last_task_retrieved - e->time_first_task_dispatched;

			debug(D_VINE, "Node %" PRIu64 " completed with outfile %s size: %zu bytes", node->node_id, node->outfile_remote_name, node->outfile_size_bytes);

			int first_completion = !node->completed; // count user progress once per graph node
			node->completed = 1;

			propagate_cut_from(e, node);
			apply_prune_depth_from(e, node);

			if (first_completion) {
				if (user_tasks_part->current == 0) {
					progress_bar_set_start_time(pbar, task->time_when_commit_start);
				}

				node_update_critical_path_time(node, task->time_workers_execute_last);
				progress_bar_update_part(pbar, user_tasks_part, 1);
			}

			if (e->failure_injection_step_percent > 0) {
				// test hook, drop workers at stepped progress thresholds
				double progress = (double)user_tasks_part->current / (double)user_tasks_part->total;
				if (progress >= next_failure_threshold && release_random_worker(e->manager)) {
					debug(D_VINE, "released a random worker at %.2f%% (threshold %.2f%%)", progress * 100, next_failure_threshold * 100);
					next_failure_threshold += e->failure_injection_step_percent / 100.0;
				}
			}

			switch (node->outfile_type) {
			case NODE_OUTFILE_TYPE_TEMP:
				vine_temp_queue_for_replication(e->manager, node->outfile); // optional replication policy
				break;
			case NODE_OUTFILE_TYPE_LOCAL:
			case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
				break;
			}

			submit_unblocked_children(e, node);
		} else {
			wait_timeout = 1; // no task ready, wait with default blocking timeout
		}
	}

	progress_bar_finish(pbar);
	progress_bar_delete(pbar);

	debug(D_VINE, "total time spent on cut propagation: %.6f seconds\n", e->time_spent_on_cut_propagation / 1e6);

	signal(SIGINT, previous_sigint_handler);
	if (interrupted) {
		raise(SIGINT); // restore handler first, then honor prior interrupt
	}
}
