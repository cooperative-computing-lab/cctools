#ifndef DAGVINE_EXECUTOR_H
#define DAGVINE_EXECUTOR_H

#include "graph.h"

#include "vine_manager.h"

typedef enum {
	TASK_PRIORITY_MODE_RANDOM = 0,
	TASK_PRIORITY_MODE_DEPTH_FIRST,
	TASK_PRIORITY_MODE_BREADTH_FIRST,
	TASK_PRIORITY_MODE_FIFO,
	TASK_PRIORITY_MODE_LIFO,
	TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST,
	TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST
} task_priority_mode_t;

struct executor {
	struct graph *graph;	      // DAG executed by this executor
	struct vine_manager *manager; // TaskVine runtime

	struct itable *task_id_to_node; // maps vine task id to graph node after submit
	struct list *resubmit_queue;	// nodes waiting for retry

	timestamp_t time_first_task_dispatched;	   // earliest dispatch time among user tasks
	timestamp_t time_last_task_retrieved;	   // latest user task retrieval time
	timestamp_t makespan_us;		   // workflow span in microseconds
	timestamp_t time_spent_on_cut_propagation; // time spent in cut propagation
	uint64_t completed_recovery_tasks;	   // recovery completions seen this run
	uint64_t pfs_usage_bytes;		   // bytes credited for shared filesystem outputs
	/** Sum of @c submit_node_task preprocessing intervals across all nodes (microseconds). */
	uint64_t total_preprocessing_time_us;
	/** Sum of @c executor_run_completion_postprocess intervals across all completions (microseconds). */
	uint64_t total_postprocessing_time_us;

	task_priority_mode_t task_priority_mode; // schedule order before submit
	double failure_injection_step_percent;	 // optional worker release steps for tests
	double progress_bar_update_interval_sec;
	int enable_debug_log;
};

struct executor *executor_create(struct vine_manager *manager, struct graph *graph);
struct graph *executor_graph_create(struct vine_manager *manager);
void executor_delete(struct executor *e);
uint64_t executor_add_node(struct executor *e);
void executor_finalize(struct executor *e);
void executor_add_task_input(struct executor *e, uint64_t task_id, const char *filename);
void executor_add_task_output(struct executor *e, uint64_t task_id, const char *filename);

int executor_tune(struct executor *e, const char *name, const char *value);
void executor_execute(struct executor *e);
uint64_t executor_get_makespan_us(const struct executor *e);
uint64_t executor_get_total_recovery_tasks(const struct executor *e);
uint64_t executor_get_completed_recovery_tasks(const struct executor *e);

#endif // DAGVINE_EXECUTOR_H
