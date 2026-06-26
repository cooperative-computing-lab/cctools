#include <inttypes.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "buffer.h"
#include "debug.h"
#include "vine_graph_executor.h"
#include "macros.h"
#include "progress_bar.h"
#include "random.h"
#include "set.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "taskvine.h"

static volatile sig_atomic_t interrupted = 0;

static void vine_graph_executor_submit_node(struct vine_graph_executor *e, struct vine_graph_node *node);
static struct vine_task *vine_graph_executor_make_vine_task(struct vine_graph_executor *e);
static void vine_graph_executor_materialize_node(struct vine_graph_executor *e, struct vine_graph_node *node);
static void vine_graph_executor_run_completion_postprocess(struct vine_graph_executor *e, struct vine_graph_node *node);

static uint64_t vine_graph_executor_count_completed_user_nodes(const struct vine_graph *g)
{
	uint64_t n = 0;
	uint64_t nid;
	struct vine_graph_node *nd;
	int iteration;

	if (!g) {
		return 0;
	}
	ITABLE_ITERATE(g->nodes, iteration, nid, nd)
	{
		if (nd->completed) {
			n++;
		}
	}
	return n;
}

/*
 * The leader's task has passed validation. Mark that node completed and, when chain grouping
 * applies, mark every non-leader in the same group. Those members did not receive their own
 * vine tasks because they ran inside the leader's single submission.
 */
static void vine_graph_executor_mark_user_node_completed_after_success(struct vine_graph *g, struct vine_graph_node *leader)
{
	if (!g || !leader) {
		return;
	}

	leader->completed = 1;

	if (!g->chain_grouping_enabled) {
		return;
	}

	struct list *smems = vine_graph_supernode_nonleader_members(g, leader->node_id);
	if (!smems) {
		return;
	}

	struct vine_graph_node *m;
	LIST_ITERATE(smems, m)
	{
		if (!m->completed) {
			m->completed = 1;
		}
	}
}

/*
 * Build the JSON payload for the runner infile argument. Field fn_args[0] names the scheduler
 * keys that run_scheduler_keys should execute. A merged chain passes one comma-separated string
 * such as "1,2,3". A singleton passes a single id. The payload must be one JSON string in that
 * slot instead of an array that mixes strings and bare numbers.
 */
static char *vine_graph_executor_format_runner_infile_json(struct vine_graph *g, struct vine_graph_node *node)
{
	if (!g || !node) {
		return NULL;
	}

	if (!g->chain_grouping_enabled) {
		return string_format("{\"fn_args\":[\"%" PRIu64 "\"],\"fn_kwargs\":{}}", node->node_id);
	}

	struct list *mems = vine_graph_supernode_nonleader_members(g, node->node_id);
	if (!mems || list_size(mems) == 0) {
		return string_format("{\"fn_args\":[\"%" PRIu64 "\"],\"fn_kwargs\":{}}", node->node_id);
	}

	buffer_t buf;
	buffer_init(&buf);
	buffer_printf(&buf, "{\"fn_args\":[\"%" PRIu64 "", node->node_id);
	struct vine_graph_node *m;
	LIST_ITERATE(mems, m)
	{
		buffer_printf(&buf, ",%" PRIu64 "", m->node_id);
	}
	buffer_printf(&buf, "\"],\"fn_kwargs\":{}}");

	char *s = xxstrdup(buffer_tostring(&buf));
	buffer_free(&buf);
	return s;
}

static void vine_graph_io_mount_add(struct list *lst, struct vine_file *f, const char *remote_name)
{
	struct vine_graph_io_mount *m = xxmalloc(sizeof(*m));
	m->file = f;
	m->remote_name = xxstrdup(remote_name);
	list_push_tail(lst, m);
}

/* Undeclare runner infile buffer (before discarding the vine_task). */
static void vine_graph_executor_clear_node_runner_arg(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (!e || !node || !node->task_runner_arg_file) {
		return;
	}
	vine_undeclare_file(e->manager, node->task_runner_arg_file);
	node->task_runner_arg_file = NULL;
}

static void vine_graph_executor_undeclare_extra_io_mounts(struct vine_graph_executor *e, struct list *mounts, struct set *seen_files)
{
	if (!e || !mounts || !seen_files) {
		return;
	}

	struct vine_graph_io_mount *m;
	LIST_ITERATE(mounts, m)
	{
		if (!m || !m->file) {
			continue;
		}
		if (!set_lookup(seen_files, m->file)) {
			set_insert(seen_files, m->file);
			vine_undeclare_file(e->manager, m->file);
		}
		m->file = NULL;
	}
}

/* Initialize runtime fields and default tuning values for a new executor. */
static void vine_graph_executor_init_runtime(struct vine_graph_executor *e)
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
	e->total_preprocessing_time_us = 0;
	e->total_postprocessing_time_us = 0;
	e->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST;
	e->failure_injection_step_percent = -1.0;
	e->progress_bar_update_interval_sec = 0.1;
}

static int vine_graph_task_not_submitted(struct vine_task *task)
{
	return !task || vine_task_get_id(task) <= 0;
}

/* Release the task-id lookup table and the resubmit queue. */
static void vine_graph_executor_clear_runtime(struct vine_graph_executor *e)
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
struct vine_graph_executor *vine_graph_executor_create(struct vine_manager *manager, struct vine_graph *graph)
{
	if (!manager || !graph) {
		return NULL;
	}

	struct vine_graph_executor *e = malloc(sizeof(*e));
	if (!e) {
		return NULL;
	}

	e->graph = graph;
	e->manager = manager;
	vine_graph_executor_init_runtime(e);
	return e;
}

/* Create a new graph for the manager's runtime directory. */
struct vine_graph *vine_graph_executor_create_graph(struct vine_manager *manager)
{
	if (!manager) {
		return NULL;
	}

	const char *runtime_dir = vine_get_runtime_directory(manager);
	return vine_graph_create(runtime_dir);
}

/* Undeclare managed files, remove local outputs, and free the executor. */
void vine_graph_executor_delete(struct vine_graph_executor *e)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (g && e->manager) {
		struct set *extra_files = set_create(0);
		uint64_t nid;
		struct vine_graph_node *node;
		int iteration;
		ITABLE_ITERATE(g->nodes, iteration, nid, node)
		{
			if (node->task_runner_arg_file) {
				vine_undeclare_file(e->manager, node->task_runner_arg_file); // before graph free to avoid double free
				node->task_runner_arg_file = NULL;
			}
			switch (node->outfile_type) {
			case VINE_GRAPH_NODE_OUTFILE_TYPE_TEMP:
				break;
			case VINE_GRAPH_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
				if (node->outfile_remote_name) {
					unlink(node->outfile_remote_name);
				}
				break;
			case VINE_GRAPH_NODE_OUTFILE_TYPE_LOCAL:
				if (node->outfile && vine_file_source(node->outfile)) {
					unlink(vine_file_source(node->outfile));
				}
				break;
			}
			if (node->outfile) {
				hash_table_remove(g->outfile_cachename_to_node, vine_file_cached_name(node->outfile));
				vine_undeclare_file(e->manager, node->outfile);
				node->outfile = NULL;
			}
			vine_graph_executor_undeclare_extra_io_mounts(e, node->extra_inputs, extra_files);
			vine_graph_executor_undeclare_extra_io_mounts(e, node->extra_outputs, extra_files);
		}
		set_delete(extra_files);
	}
	vine_graph_executor_clear_runtime(e);
	free(e);
}

/*
 * Create a new library task (not yet published on the node). The caller attaches IO, then sets
 * node->task only when the task is fully configured (atomic materialize).
 */
static struct vine_task *vine_graph_executor_make_vine_task(struct vine_graph_executor *e)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g) {
		return NULL;
	}

	if (!g->task_runner_function_name) {
		debug(D_ERROR, "task runner function name is not set");
		vine_graph_delete(g);
		exit(1);
	}
	if (!g->task_runner_library_name) {
		debug(D_ERROR, "task runner library name is not set");
		vine_graph_delete(g);
		exit(1);
	}

	struct vine_task *t = vine_task_create(g->task_runner_function_name);
	vine_task_set_library_required(t, g->task_runner_library_name);
	vine_task_addref(t); // keep alive across vine_submit and vine_wait
	return t;
}

/*
 * Attach inputs, outputs, and the infile buffer to a new vine_task at submit time. The node's
 * task pointer stays NULL until that bundle is complete and ready for vine_submit.
 */
static void vine_graph_executor_materialize_node(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}

	if (node->task && !vine_graph_task_not_submitted(node->task)) {
		return;
	}
	/* INITIAL task implies mounts + runner infile are already complete. */
	if (node->task) {
		return;
	}

	vine_graph_executor_clear_node_runner_arg(e, node);

	struct vine_task *t = vine_graph_executor_make_vine_task(e);
	if (!t) {
		return;
	}

	if (node->outfile) {
		vine_task_add_output(t, node->outfile, node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
	}

	void *item;
	LIST_ITERATE(node->extra_outputs, item)
	{
		struct vine_graph_io_mount *m = (struct vine_graph_io_mount *)item;
		vine_task_add_output(t, m->file, m->remote_name, VINE_TRANSFER_ALWAYS);
	}

	/*
	 * When chains are merged, only the leader submits a task, but that task must list every
	 * member's declared outputs so the manager can track each outfile by node id.
	 */
	if (g->chain_grouping_enabled && vine_graph_node_is_supernode_leader(node)) {
		struct list *mems = vine_graph_supernode_nonleader_members(g, node->node_id);
		if (mems) {
			struct vine_graph_node *m;
			LIST_ITERATE(mems, m)
			{
				if (m->outfile) {
					vine_task_add_output(t, m->outfile, m->outfile_remote_name, VINE_TRANSFER_ALWAYS);
				}
				LIST_ITERATE(m->extra_outputs, item)
				{
					struct vine_graph_io_mount *em = (struct vine_graph_io_mount *)item;
					vine_task_add_output(t, em->file, em->remote_name, VINE_TRANSFER_ALWAYS);
				}
			}
		}
	}

	struct vine_graph_node *parent_node;
	LIST_ITERATE(node->parents, parent_node)
	{
		struct vine_graph_node *src = vine_graph_input_producer_node(g, parent_node, node);
		if (src && src->outfile) {
			vine_task_add_input(t, src->outfile, src->outfile_remote_name, VINE_TRANSFER_ALWAYS);
		}
	}

	LIST_ITERATE(node->extra_inputs, item)
	{
		struct vine_graph_io_mount *m = (struct vine_graph_io_mount *)item;
		vine_task_add_input(t, m->file, m->remote_name, VINE_TRANSFER_ALWAYS);
	}

	char *task_arguments = vine_graph_executor_format_runner_infile_json(g, node);
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
static void vine_graph_executor_declare_node_outfile(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !node || node->outfile) {
		return;
	}

	switch (node->outfile_type) {
	case VINE_GRAPH_NODE_OUTFILE_TYPE_LOCAL: {
		char *local_outfile_path = string_format("%s/%s", g->output_dir, node->outfile_remote_name);
		node->outfile = vine_declare_file(e->manager, local_outfile_path, VINE_CACHE_LEVEL_WORKFLOW, 0);
		free(local_outfile_path);
		break;
	}
	case VINE_GRAPH_NODE_OUTFILE_TYPE_TEMP:
		node->outfile = vine_declare_temp(e->manager);
		break;
	case VINE_GRAPH_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
		break;
	}
}

/*
 * Allocate the next graph node. Per-node vine_task and I/O mounts appear later during
 * vine_graph_executor_materialize_node at submit time.
 */
uint64_t vine_graph_executor_add_node(struct vine_graph_executor *e)
{
	if (!e || !e->graph) {
		return 0;
	}

	uint64_t node_id = vine_graph_add_node(e->graph);
	return node_id;
}

/*
 * Finalize the graph: declare outputs with cached_name registration,
 * attach parent inputs, and set remaining parent counts for scheduling.
 */
void vine_graph_executor_finalize(struct vine_graph_executor *e)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g) {
		return;
	}

	vine_graph_finalize(g);

	/*
	 * Two passes. Declare outputs and cached_name map first so parent
	 * vine_file objects exist. Task-level input/output mounts are applied
	 * in vine_graph_executor_materialize_node at submit time.
	 */
	uint64_t nid;
	struct vine_graph_node *node;
	int iteration;
	ITABLE_ITERATE(g->nodes, iteration, nid, node)
	{
		vine_graph_executor_declare_node_outfile(e, node);
		if (node->outfile) {
			hash_table_insert(g->outfile_cachename_to_node, vine_file_cached_name(node->outfile), node);
		}
	}

	ITABLE_ITERATE(g->nodes, iteration, nid, node)
	{
		node->remaining_parents_count = list_size(node->parents);
	}
}

/* Add a named input, reusing a declared file when the logical filename was already mapped. */
void vine_graph_executor_add_task_input(struct vine_graph_executor *e, uint64_t task_id, const char *filename)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !task_id || !filename) {
		return;
	}

	struct vine_graph_node *node = itable_lookup(g->nodes, task_id);
	if (!node) {
		return;
	}

	struct vine_file *f = NULL;
	const char *cached_name = hash_table_lookup(g->inout_filename_to_cached_name, filename);

	if (cached_name) {
		f = vine_manager_lookup_file(e->manager, cached_name);
	} else {
		f = vine_declare_temp(e->manager); // first use of logical name, record cache key for paired mounts
		hash_table_insert(g->inout_filename_to_cached_name, filename, xxstrdup(vine_file_cached_name(f)));
	}

	vine_graph_io_mount_add(node->extra_inputs, f, filename);
}

/* Add a named output, linking logical filename to a cache name for paired producer and consumer tasks. */
void vine_graph_executor_add_task_output(struct vine_graph_executor *e, uint64_t task_id, const char *filename)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !task_id || !filename) {
		return;
	}

	struct vine_graph_node *node = itable_lookup(g->nodes, task_id);
	if (!node) {
		return;
	}

	struct vine_file *f = NULL;
	const char *cached_name = hash_table_lookup(g->inout_filename_to_cached_name, filename);

	if (cached_name) {
		f = vine_manager_lookup_file(e->manager, cached_name);
	} else {
		f = vine_declare_temp(e->manager); // pair with matching input on consumer tasks
		hash_table_insert(g->inout_filename_to_cached_name, filename, xxstrdup(vine_file_cached_name(f)));
	}

	vine_graph_io_mount_add(node->extra_outputs, f, filename);
}

/* Apply executor-level tuning. Unknown keys are forwarded to vine_graph_tune. */
int vine_graph_executor_tune(struct vine_graph_executor *e, const char *name, const char *value)
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

	} else {
		return vine_graph_tune(e->graph, name, value);
	}

	return 0;
}

/* Set the interrupted flag when SIGINT is received. */
static void vine_graph_executor_handle_sigint(int signal)
{
	interrupted = 1;
}

/* Compute submission priority for a node using the configured scheduling policy. */
static double vine_graph_executor_calculate_task_priority(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!node || !g) {
		return 0;
	}

	double priority = 0;
	timestamp_t current_time = timestamp_get();
	struct vine_graph_node *parent_node;

	switch (e->task_priority_mode) {
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
			struct vine_graph_node *src = vine_graph_input_producer_node(g, parent_node, node);
			if (!src || !src->outfile) {
				continue;
			}
			priority += (double)vine_file_size(src->outfile);
		}
		break;
	case TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST:
		LIST_ITERATE(node->parents, parent_node)
		{
			struct vine_graph_node *src = vine_graph_input_producer_node(g, parent_node, node);
			if (!src || !src->outfile) {
				continue;
			}
			if (!parent_node->task) {
				continue;
			}
			timestamp_t parent_task_completion_time = vine_task_get_metric(parent_node->task, "time_workers_execute_last");
			priority += (double)vine_file_size(src->outfile) * (double)parent_task_completion_time;
		}
		break;
	}

	return priority;
}

/* Submit the node task if it is still initial, and record the manager task id for later lookup. */
static void vine_graph_executor_submit_node(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}

	timestamp_t t_pre = timestamp_get();

	vine_graph_executor_materialize_node(e, node);

	if (!node->task) {
		debug(D_ERROR, "vine_graph_executor_submit_node: node %" PRIu64 " has no task after materialize", node->node_id);
		goto record_preprocessing;
	}

	if (!vine_graph_task_not_submitted(node->task)) {
		debug(D_VINE,
				"vine_graph_executor_submit_node: skipping node %" PRIu64 " (task already submitted, state=%s, task_id=%d)",
				node->node_id,
				vine_task_get_state(node->task),
				vine_task_get_id(node->task));
		goto record_preprocessing;
	}

	double priority = vine_graph_executor_calculate_task_priority(e, node);
	vine_task_set_priority(node->task, priority);

	int task_id = vine_submit(e->manager, node->task);

	if (task_id <= 0) {
		debug(D_ERROR, "vine_graph_executor_submit_node: failed to submit node %" PRIu64 " (returned task_id=%d)", node->node_id, task_id);
		goto record_preprocessing;
	}

	itable_insert(e->task_id_to_node, (uint64_t)task_id, node); // reverse lookup from vine_wait
	debug(D_VINE, "submitted node %" PRIu64 " with task id %d", node->node_id, task_id);

record_preprocessing: {
	uint64_t dt = (uint64_t)(timestamp_get() - t_pre);
	node->preprocessing_time_us = dt;
	e->total_preprocessing_time_us += dt;
	debug(D_VINE,
			"node %" PRIu64 " preprocessing %" PRIu64 " us, graph cumulative %" PRIu64 " us",
			node->node_id,
			dt,
			e->total_preprocessing_time_us);
}
}

/*
 * Return true when this node is allowed to submit. If chain grouping is active, only the
 * supernode leader may submit because other members are executed inside the leader's task.
 */
static int vine_graph_node_ready_for_submission(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!e || !node || node->remaining_parents_count != 0 || node->completed) {
		return 0;
	}
	if (g && g->chain_grouping_enabled && !vine_graph_node_is_supernode_leader(node)) {
		return 0;
	}
	if (node->in_resubmit_queue) {
		return 0;
	}
	if (node->task && !vine_graph_task_not_submitted(node->task)) {
		return 0;
	}
	return 1;
}

/* Submit ready source nodes and enable delivery of recovery tasks to the application. */
static void vine_graph_executor_submit_initial_ready_nodes(struct vine_graph_executor *e)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !e->manager) {
		return;
	}

	uint64_t nid;
	struct vine_graph_node *node;
	int iteration;
	ITABLE_ITERATE(g->nodes, iteration, nid, node)
	{
		if (vine_graph_node_ready_for_submission(e, node)) {
			vine_graph_executor_submit_node(e, node);
		}
	}

	vine_enable_external_recovery_handling(e->manager); // driver must observe recovery completions for cut and prune
}

/* After one parent completes, decrement remaining parents and submit children that become ready. */
static void vine_graph_executor_submit_unblocked_children(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}

	struct vine_graph_node *child_node;
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

		if (vine_graph_node_ready_for_submission(e, child_node)) {
			vine_graph_executor_submit_node(e, child_node);
		}
	}
}

/* Map a completed vine_task to the corresponding graph node, including recovery tasks. */
static struct vine_graph_node *vine_graph_executor_node_from_task(struct vine_graph_executor *e, struct vine_task *task)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !task) {
		return NULL;
	}

	/* Recovery completions map to the original producer; regular completions map to themselves. */
	int lookup_task_id = vine_task_get_recovery_source_task_id(task);
	if (lookup_task_id <= 0) {
		lookup_task_id = vine_task_get_id(task);
	}
	if (lookup_task_id > 0) {
		return itable_lookup(e->task_id_to_node, (uint64_t)lookup_task_id);
	}

	debug(D_ERROR, "task %d has no graph node mapping", vine_task_get_id(task));
	return NULL;
}

/* Update shared-filesystem byte counters when a node's credited output size changes. */
static void vine_graph_executor_account_pfs_write(struct vine_graph_executor *e, struct vine_graph_node *n, size_t new_size)
{
	struct vine_graph *g = e ? e->graph : NULL;
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
static void vine_graph_executor_account_pfs_delete(struct vine_graph_executor *e, struct vine_graph_node *n)
{
	struct vine_graph *g = e ? e->graph : NULL;
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
static int vine_graph_node_is_anchored(const struct vine_graph_node *n)
{
	if (!n || !n->completed) {
		return 0;
	}
	return n->outfile_type == VINE_GRAPH_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM || n->outfile_type == VINE_GRAPH_NODE_OUTFILE_TYPE_LOCAL;
}

/* Return non-zero when a temporary output file has a recovery task that is neither initial nor finished. */
static int vine_graph_node_is_mid_recovery(const struct vine_graph_node *n)
{
	if (!n || !n->outfile || vine_file_type(n->outfile) != VINE_TEMP) {
		return 0;
	}
	return vine_file_is_recovering(n->outfile);
}

/* Remove or prune the node's result file according to its output storage mode. */
static void vine_graph_executor_delete_node_output(struct vine_graph_executor *e, struct vine_graph_node *n)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !n) {
		return;
	}

	switch (n->outfile_type) {
	case VINE_GRAPH_NODE_OUTFILE_TYPE_TEMP:
		if (n->outfile) {
			vine_prune_file(e->manager, n->outfile);
		}
		break;
	case VINE_GRAPH_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
		vine_graph_executor_account_pfs_delete(e, n);
		if (n->outfile_remote_name) {
			unlink(n->outfile_remote_name);
		}
		break;
	case VINE_GRAPH_NODE_OUTFILE_TYPE_LOCAL:
		if (n->outfile && vine_file_source(n->outfile)) {
			unlink(vine_file_source(n->outfile));
		}
		break;
	}
}

/* Attempt to mark a completed node as cut and delete its return file when all children permit release. */
static int vine_graph_executor_try_cut_node(struct vine_graph_executor *e, struct vine_graph_node *n)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !n || n->cut || !n->completed) {
		return 0;
	}

	struct vine_graph_node *c;
	LIST_ITERATE(n->children, c)
	{
		if ((!vine_graph_node_is_anchored(c) && !c->cut) || vine_graph_node_is_mid_recovery(c)) {
			return 0; // wait for anchored, cut, or non-recovery children
		}
	}

	n->cut = 1;
	debug(D_VINE, "cut: node %" PRIu64 " outfile_type=%d is_target=%d", n->node_id, n->outfile_type, n->is_target);

	if (!n->is_target) {
		vine_graph_executor_delete_node_output(e, n); // targets keep data for retrieval
	}

	return 1;
}

/* Walk upstream from a completed node and apply cut propagation along the worklist. */
static void vine_graph_executor_propagate_cut_from(struct vine_graph_executor *e, struct vine_graph_node *start)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !start || !start->completed) {
		return;
	}

	timestamp_t t0 = timestamp_get();
	vine_graph_executor_try_cut_node(e, start);

	/* Upstream BFS: when a node is cut, enqueue its parents for the same check. */
	struct list *worklist = list_create();
	struct vine_graph_node *p;
	LIST_ITERATE(start->parents, p)
	{
		list_push_tail(worklist, p);
	}

	while (list_size(worklist) > 0) {
		struct vine_graph_node *m = list_pop_head(worklist);
		if (vine_graph_executor_try_cut_node(e, m)) {
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
static int vine_graph_node_descendants_completed_within_depth(struct vine_graph_node *a, int depth)
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
		struct vine_graph_node *n;
		LIST_ITERATE(current, n)
		{
			struct vine_graph_node *c;
			LIST_ITERATE(n->children, c)
			{
				if (set_lookup(visited, c)) {
					continue;
				}
				set_insert(visited, c);
				if (!c->completed || vine_graph_node_is_mid_recovery(c)) {
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
static void vine_graph_executor_try_prune_depth_release(struct vine_graph_executor *e, struct vine_graph_node *a)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !a) {
		return;
	}
	if (a->released_by_prune_depth) {
		return;
	}
	if (a->outfile_type != VINE_GRAPH_NODE_OUTFILE_TYPE_TEMP) {
		return;
	}
	if (a->is_target) {
		return;
	}
	if (!a->completed || !a->outfile) {
		return;
	}
	if (!vine_graph_node_descendants_completed_within_depth(a, g->prune_depth)) {
		return; // wait until descendants within prune_depth layers are settled
	}

	vine_graph_executor_delete_node_output(e, a);
	a->released_by_prune_depth = 1;

	debug(D_VINE, "prune-depth release: node %" PRIu64 " depth=%d", a->node_id, g->prune_depth);
}

/* Apply prune-depth release starting at a node and extending up to k ancestor levels. */
static void vine_graph_executor_apply_prune_depth_from(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}
	int k = g->prune_depth;
	if (k <= 0) {
		return;
	}

	vine_graph_executor_try_prune_depth_release(e, node);

	struct set *visited = set_create(0);
	struct list *current = list_create();
	list_push_tail(current, node);
	set_insert(visited, node);

	/* Visit new parents up to k levels, trying prune release on each. */
	for (int d = 1; d <= k; d++) {
		struct list *next = list_create();
		struct vine_graph_node *n;
		LIST_ITERATE(current, n)
		{
			struct vine_graph_node *p;
			LIST_ITERATE(n->parents, p)
			{
				if (set_lookup(visited, p)) {
					continue;
				}
				set_insert(visited, p);
				list_push_tail(next, p);
				vine_graph_executor_try_prune_depth_release(e, p);
			}
		}
		list_delete(current);
		current = next;
	}

	list_delete(current);
	set_delete(visited);
}

/*
 * Completion hook after a node finishes: cut propagation, prune-depth handling, and timing.
 * Postprocessing wall time is charged to the node that triggered this call.
 */
static void vine_graph_executor_run_completion_postprocess(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (!e || !node) {
		return;
	}

	timestamp_t t0 = timestamp_get();
	vine_graph_executor_propagate_cut_from(e, node);
	vine_graph_executor_apply_prune_depth_from(e, node);
	uint64_t dt = (uint64_t)(timestamp_get() - t0);
	node->postprocessing_time_us = dt;
	e->total_postprocessing_time_us += dt;
	debug(D_VINE,
			"node %" PRIu64 " postprocessing %" PRIu64 " us, graph cumulative %" PRIu64 " us",
			node->node_id,
			dt,
			e->total_postprocessing_time_us);
}

#define RESUBMIT_SCAN_LIMIT 100
#define RESUBMIT_COOLDOWN_USECS ((timestamp_t)1000000)

/*
 * Queue a retry after failure. With chain grouping the queue stores the leader so one retry
 * resubmits the whole merged task. Without grouping the failing node itself is the leader.
 */
static void vine_graph_executor_queue_node_retry(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}

	struct vine_graph_node *leader = node;
	if (g->chain_grouping_enabled) {
		struct vine_graph_node *mapped = vine_graph_supernode_leader_node(g, node);
		if (mapped) {
			leader = mapped;
		}
	}
	if (!leader) {
		return;
	}

	if (leader->in_resubmit_queue) {
		return;
	}

	leader->last_failure_time = timestamp_get();
	list_push_tail(e->resubmit_queue, leader);
	leader->in_resubmit_queue = 1;
}

/*
 * Process the resubmit queue after cooldown. A ready head is popped, its failed task is torn down,
 * and vine_graph_executor_submit_node runs again. If the head is still cooling off, rotate it to the tail so
 * other leaders behind it are not stuck forever while the driver waits.
 */
static void vine_graph_executor_drain_resubmit_queue(struct vine_graph_executor *e)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g) {
		return;
	}

	timestamp_t now = timestamp_get();
	int queued = list_size(e->resubmit_queue);
	if (queued == 0) {
		return;
	}

	int budget = queued < RESUBMIT_SCAN_LIMIT ? queued : RESUBMIT_SCAN_LIMIT;
	int resubmits = 0;
	int rotations_without_resubmit = 0;

	while (resubmits < budget && list_size(e->resubmit_queue) > 0) {
		struct vine_graph_node *node = list_peek_head(e->resubmit_queue);
		if (!node) {
			break;
		}
		if (now - node->last_failure_time >= RESUBMIT_COOLDOWN_USECS) {
			list_pop_head(e->resubmit_queue);
			node->in_resubmit_queue = 0;

			debug(D_VINE, "Resubmitting node %" PRIu64, node->node_id);
			vine_graph_executor_clear_node_runner_arg(e, node);
			if (node->task) {
				vine_task_delete(node->task);
				node->task = NULL;
			}
			vine_graph_executor_submit_node(e, node);
			resubmits++;
			rotations_without_resubmit = 0;
		} else {
			list_pop_head(e->resubmit_queue);
			list_push_tail(e->resubmit_queue, node);
			rotations_without_resubmit++;
			if (rotations_without_resubmit >= list_size(e->resubmit_queue)) {
				break;
			}
		}
	}
}

/*
 * Verify status and on-disk outputs (primary outfile only).
 * On failure enqueue a retry for the node.
 */
static int vine_graph_executor_validate_node_outputs_or_retry(struct vine_graph_executor *e, struct vine_graph_node *retry_node, struct vine_graph_node *output_node, struct vine_task *task)
{
	switch (output_node->outfile_type) {
	case VINE_GRAPH_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM: {
		struct stat info;
		// shared path may lag behind a successful task result code
		if (stat(output_node->outfile_remote_name, &info) < 0) {
			debug(D_VINE, "Task %d succeeded but missing sharedfs output %s", vine_task_get_id(task), output_node->outfile_remote_name);
			vine_graph_executor_queue_node_retry(e, retry_node);
			return 0;
		}
		output_node->outfile_size_bytes = info.st_size;
		vine_graph_executor_account_pfs_write(e, output_node, (size_t)info.st_size);
		break;
	}
	case VINE_GRAPH_NODE_OUTFILE_TYPE_LOCAL:
	case VINE_GRAPH_NODE_OUTFILE_TYPE_TEMP:
		if (output_node->outfile) {
			output_node->outfile_size_bytes = vine_file_size(output_node->outfile);
		}
		break;
	}

	return 1;
}

/*
 * Verify one extra output mount after a successful task (VINE_FILE paths must exist).
 */
static int vine_graph_executor_validate_io_mount_or_retry(
		struct vine_graph_executor *e, struct vine_graph_node *retry_node, struct vine_graph_io_mount *mount, struct vine_task *task)
{
	if (!mount || !mount->file) {
		return 1;
	}
	struct vine_file *f = mount->file;

	switch (vine_file_type(f)) {
	case VINE_TEMP:
	case VINE_BUFFER:
		break;
	case VINE_FILE:
		if (vine_file_source(f)) {
			struct stat info;
			if (stat(vine_file_source(f), &info) < 0) {
				debug(D_VINE,
						"Task %d succeeded but missing extra output file %s (%s)",
						vine_task_get_id(task),
						mount->remote_name ? mount->remote_name : "?",
						vine_file_source(f));
				vine_graph_executor_queue_node_retry(e, retry_node);
				return 0;
			}
		}
		break;
	default:
		break;
	}
	return 1;
}

/*
 * Primary outfile (per node outfile_type) plus every extra_outputs mount declared for that graph node.
 */
static int vine_graph_executor_validate_all_declared_outputs_or_retry(
		struct vine_graph_executor *e, struct vine_graph_node *retry_node, struct vine_graph_node *output_node, struct vine_task *task)
{
	if (!vine_graph_executor_validate_node_outputs_or_retry(e, retry_node, output_node, task)) {
		return 0;
	}
	void *item;
	LIST_ITERATE(output_node->extra_outputs, item)
	{
		if (!vine_graph_executor_validate_io_mount_or_retry(e, retry_node, (struct vine_graph_io_mount *)item, task)) {
			return 0;
		}
	}
	return 1;
}

static int vine_graph_executor_validate_task_or_retry(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_task *task)
{
	struct vine_graph *g = e ? e->graph : NULL;
	/*
	 * Returning zero means the completion is rejected, a retry is enqueued, and the progress
	 * bar must not advance because the batch did not truly succeed yet.
	 */
	if (vine_task_get_result(task) != VINE_RESULT_SUCCESS || vine_task_get_exit_code(task) != 0) {
		debug(D_VINE,
				"Task %d failed (result=%d, exit=%d)",
				vine_task_get_id(task),
				vine_task_get_result(task),
				vine_task_get_exit_code(task));
		vine_graph_executor_queue_node_retry(e, node);
		return 0;
	}

	if (!vine_graph_executor_validate_all_declared_outputs_or_retry(e, node, node, task)) {
		return 0;
	}

	/*
	 * One vine task may carry outputs for the whole supernode. Validate every grouped member's
	 * declared files against that same task, not only the leader's primary outfile.
	 */
	if (g && g->chain_grouping_enabled) {
		struct list *mems = vine_graph_supernode_nonleader_members(g, node->node_id);
		if (mems) {
			struct vine_graph_node *m;
			LIST_ITERATE(mems, m)
			{
				if (!vine_graph_executor_validate_all_declared_outputs_or_retry(e, node, m, task)) {
					return 0;
				}
			}
		}
	}

	return 1;
}

/* Return the recorded user-task makespan in microseconds. */
uint64_t vine_graph_executor_get_makespan_us(const struct vine_graph_executor *e)
{
	if (!e) {
		return 0;
	}

	return (uint64_t)e->makespan_us;
}

/* Return the manager's cumulative count of recovery tasks submitted. */
uint64_t vine_graph_executor_get_total_recovery_tasks(const struct vine_graph_executor *e)
{
	if (!e || !e->manager) {
		return 0;
	}

	struct vine_stats stats;
	vine_get_stats(e->manager, &stats);
	return (uint64_t)stats.tasks_recovery;
}

/* Return how many recovery tasks have completed in the current executor run. */
uint64_t vine_graph_executor_get_completed_recovery_tasks(const struct vine_graph_executor *e)
{
	if (!e) {
		return 0;
	}

	return e->completed_recovery_tasks;
}

/* Main loop: submit work, wait, handle recovery, update graph state. */
void vine_graph_executor_execute(struct vine_graph_executor *e)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g) {
		return;
	}

	interrupted = 0;
	void (*previous_sigint_handler)(int) = signal(SIGINT, vine_graph_executor_handle_sigint);

	debug(D_VINE, "start executing executor graph");

	vine_graph_executor_submit_initial_ready_nodes(e);

	struct ProgressBar *pbar = progress_bar_init("Executing Tasks");
	progress_bar_set_update_interval(pbar, e->progress_bar_update_interval_sec);
	e->completed_recovery_tasks = 0;

	struct ProgressBarPart *user_tasks_part = progress_bar_create_part("User", itable_size(g->nodes));
	struct ProgressBarPart *recovery_tasks_part = progress_bar_create_part("Recovery", 0);
	progress_bar_bind_part(pbar, user_tasks_part);
	progress_bar_bind_part(pbar, recovery_tasks_part);

	const uint64_t user_node_total = itable_size(g->nodes);

	double next_failure_threshold = -1.0;
	if (e->failure_injection_step_percent > 0) {
		next_failure_threshold = e->failure_injection_step_percent / 100.0;
	}

	int wait_timeout = 1; // short timeout after a result, longer when idle

	/*
	 * Stop the main loop once every graph node reports completed. Count completed nodes directly
	 * instead of relying on progress bar ticks because grouped members can flip to completed in
	 * a single completion event and the bar would miss intermediate steps.
	 */
	while (vine_graph_executor_count_completed_user_nodes(g) < user_node_total) {
		if (interrupted) {
			break;
		}

		vine_graph_executor_drain_resubmit_queue(e);
		progress_bar_set_part_total(pbar, recovery_tasks_part, vine_graph_executor_get_total_recovery_tasks(e));

		struct vine_task *task = vine_wait(e->manager, wait_timeout);
		if (task) {
			wait_timeout = 0;

			struct vine_graph_node *node = vine_graph_executor_node_from_task(e, task);
			if (!node) {
				debug(D_ERROR, "fatal: task %d could not be mapped to a task node, this indicates a serious bug.", vine_task_get_id(task));
				exit(1);
			}

			timestamp_t commit_end = vine_task_get_metric(task, "time_when_commit_end");
			if (commit_end > 0) {
				e->time_first_task_dispatched = MIN(e->time_first_task_dispatched, commit_end); // makespan start
			}

			/*
			 * User and recovery progress advances only after outputs validate. Failed tasks enter
			 * the retry path and leave the bar unchanged until a later successful completion.
			 */
			if (!vine_graph_executor_validate_task_or_retry(e, node, task)) {
				continue;
			}

			int first_completion = 0;

			if (vine_task_get_recovery_source_task_id(task) > 0) {
				e->completed_recovery_tasks++;
				progress_bar_update_part(
						pbar,
						recovery_tasks_part,
						e->completed_recovery_tasks - recovery_tasks_part->current);

				/* Reset cut and prune-depth flags for recovery tasks. */
				node->cut = 0;
				node->released_by_prune_depth = 0;

				/* Only postprocess recovery tasks. */
				vine_graph_executor_run_completion_postprocess(e, node);
			} else {
				timestamp_t retrieval_time = (timestamp_t)vine_task_get_metric(task, "time_when_retrieval");
				e->time_last_task_retrieved = MAX(e->time_last_task_retrieved, retrieval_time);
				e->makespan_us = e->time_last_task_retrieved - e->time_first_task_dispatched;

				first_completion = !node->completed;
				vine_graph_executor_mark_user_node_completed_after_success(g, node);

				if (first_completion) {
					if (user_tasks_part->current == 0) {
						progress_bar_set_start_time(pbar, vine_task_get_metric(task, "time_when_commit_start"));
					}

					vine_graph_node_update_critical_path_time(node, vine_task_get_metric(task, "time_workers_execute_last"));
				}

				uint64_t completed_user_nodes = vine_graph_executor_count_completed_user_nodes(g);
				if (completed_user_nodes > user_tasks_part->current) {
					/* Count graph nodes, then advance by the delta to avoid double-counting retries. */
					progress_bar_update_part(pbar, user_tasks_part, completed_user_nodes - user_tasks_part->current);
				}

				if (e->failure_injection_step_percent > 0) {
					// test hook, drop workers at stepped progress thresholds
					double progress = (double)user_tasks_part->current / (double)user_tasks_part->total;
					if (progress >= next_failure_threshold && vine_manager_release_random_worker(e->manager)) {
						debug(D_VINE, "released a random worker at %.2f%% (threshold %.2f%%)", progress * 100, next_failure_threshold * 100);
						next_failure_threshold += e->failure_injection_step_percent / 100.0;
					}
				}

				/* Postprocess the node and submit its children. */
				vine_graph_executor_run_completion_postprocess(e, node);
				vine_graph_executor_submit_unblocked_children(e, node);
			}
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
