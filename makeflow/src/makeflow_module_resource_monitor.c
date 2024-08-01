
#include "create_dir.h"
#include "debug.h"
#include "jx.h"
#include "path.h"
#include "rmonitor.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "batch_job.h"
#include "batch_wrapper.h"

#include "dag.h"
#include "dag_file.h"
#include "dag_node.h"
#include "makeflow_hook.h"
#include "makeflow_log.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEFAULT_MONITOR_LOG_FORMAT "resource-rule-%%"

static int instances = 0;

struct makeflow_monitor {
	int enable_debug;
	int enable_time_series;
	int enable_list_files;

	int interval;
	char *measure_dir;
	char *log_dir;
	char *log_format;
	char *log_prefix;
	char *exe;
	const char *exe_remote;

	int instance;
};

struct makeflow_monitor *makeflow_monitor_create()
{
	struct makeflow_monitor *m = malloc(sizeof(*m));
	m->enable_debug = 0;
	m->enable_time_series = 0;
	m->enable_list_files = 0;

	m->interval = 1; // in seconds
	m->measure_dir = NULL;
	m->log_dir = NULL;
	m->log_format = NULL;
	m->log_prefix = NULL;
	m->exe = NULL;
	m->exe_remote = NULL;

	m->instance = instances++;

	return m;
}

static int resource_monitor_register_hook(struct makeflow_hook *h, struct list *hooks, struct jx **args)
{
	struct makeflow_hook *tail = list_pop_tail(hooks);
	if(tail){
		list_push_tail(hooks, tail);
		if(!strcmp(h->module_name, tail->module_name)){
			return MAKEFLOW_HOOK_SKIP;
		}
	}
	return MAKEFLOW_HOOK_SUCCESS;
}

static int create(void ** instance_struct, struct jx *args)
{
	struct makeflow_monitor *monitor = makeflow_monitor_create();
	*instance_struct = monitor;

	if (jx_lookup_string(args, "resource_monitor_exe")){
		monitor->exe = xxstrdup(jx_lookup_string(args, "resource_monitor_exe"));
	} else {
		monitor->exe = resource_monitor_locate(NULL);
	}

	if (jx_lookup_string(args, "resource_monitor_log_dir"))
		monitor->log_dir = xxstrdup(jx_lookup_string(args, "resource_monitor_log_dir"));

	if (jx_lookup_string(args, "resource_monitor_log_format"))
		monitor->log_format = xxstrdup(jx_lookup_string(args, "resource_monitor_log_format"));

	if(jx_lookup_integer(args, "resource_monitor_interval"))
		monitor->interval = jx_lookup_integer(args, "resource_monitor_interval");

	if(jx_lookup_integer(args, "resource_monitor_measure_dir"))
		monitor->measure_dir = xxstrdup("$PWD");

	monitor->enable_time_series = jx_lookup_integer(args, "resource_monitor_enable_time_series");
	monitor->enable_list_files = jx_lookup_integer(args, "resource_monitor_enable_list_files");

	if (!monitor->log_dir) {
		debug(D_ERROR|D_MAKEFLOW_HOOK,"Monitor mode was enabled, but a log output directory was not specified (use --monitor=<dir>)");
		return MAKEFLOW_HOOK_FAILURE;
	}

	if (!monitor->log_format)
		monitor->log_format = xxstrdup(DEFAULT_MONITOR_LOG_FORMAT);

	monitor->log_prefix = string_format("%s/%s", monitor->log_dir, monitor->log_format);

	if (monitor->interval < 1) {
		debug(D_ERROR|D_MAKEFLOW_HOOK,"Monitoring interval should be positive.");
		return MAKEFLOW_HOOK_FAILURE;
	}

	if (!monitor->exe) {
		debug(D_ERROR|D_MAKEFLOW_HOOK,"Monitor mode was enabled, but could not find resource_monitor in PATH.");
		return MAKEFLOW_HOOK_FAILURE;
	}

	monitor->exe_remote = xxstrdup("cctools-monitor");

	return MAKEFLOW_HOOK_SUCCESS;
}

static int destroy(void * instance_struct, struct dag *d)
{
	struct makeflow_monitor *monitor = (struct makeflow_monitor*)instance_struct;
	if(monitor){
		if (monitor->log_prefix)
			free(monitor->log_prefix);

		if (monitor->exe)
			free(monitor->exe);

		free(monitor->measure_dir);
		free(monitor);
	}
	return MAKEFLOW_HOOK_SUCCESS;
}

static int dag_start(void * instance_struct, struct dag *d)
{
	struct makeflow_monitor *monitor = (struct makeflow_monitor*)instance_struct;
	dag_file_lookup_or_create(d, monitor->exe);

	int result = mkdir(monitor->log_dir, 0777);
	if (result == -1) {
		if (errno == ENOENT) {
			result = !create_dir(monitor->log_dir, 0777);
		} else if (errno != EEXIST) {
			debug(D_ERROR|D_MAKEFLOW_HOOK,"Monitor mode was enabled, but could not create output directory. %s", strerror(errno));
		}
	}
	if (result == 0) { // Either the mkdir was successful, or create_dir was successful. aka created in Makeflow
		struct dag_file *f = dag_file_lookup_or_create(d, monitor->log_dir);
		makeflow_log_file_state_change(d, f, DAG_FILE_STATE_EXISTS);
	}

	return MAKEFLOW_HOOK_SUCCESS;
}

/* Helper function to consistently create prefix. Free returned char *. */
static char *set_log_prefix(struct makeflow_monitor *monitor, struct dag_node *n)
{
	char *nodeid = string_format("%d", n->nodeid);
	char *log_prefix = string_replace_percents(monitor->log_prefix, nodeid);
	free(nodeid);
	return log_prefix;
}

static int node_submit(void * instance_struct, struct dag_node *n, struct batch_job *task)
{
	struct makeflow_monitor *monitor = (struct makeflow_monitor*)instance_struct;
	char *log_name;
	char *executable = NULL;

	struct batch_wrapper *wrapper = batch_wrapper_create();
	char *prefix = string_format("./resource_monitor_%d", n->nodeid);
	batch_wrapper_prefix(wrapper, prefix);
	free(prefix);

	// Add/Use the existing executable that has been used for previous nodes.
	makeflow_hook_add_input_file(n->d, task, monitor->exe, monitor->exe_remote, DAG_FILE_TYPE_GLOBAL);

	// If the queue supports remote_renaming add as remote rename.
	if (batch_queue_supports_feature(makeflow_get_queue(n), "remote_rename")) {
		executable = string_format("./%s", monitor->exe_remote);
	} else {
		// Else just use executable in path
		executable = string_format("%s", monitor->exe);
	}

	char *log_prefix = set_log_prefix(monitor, n);

	// Format and add summary
	log_name = string_format("%s.summary", log_prefix);
	makeflow_hook_add_output_file(n->d, task, log_name, NULL, DAG_FILE_TYPE_INTERMEDIATE);
	free(log_name);

	// Format and add series
	if (monitor->enable_time_series) {
		log_name = string_format("%s.series", log_prefix);
		makeflow_hook_add_output_file(n->d, task, log_name, NULL, DAG_FILE_TYPE_INTERMEDIATE);
		free(log_name);
	}

	// Format and add file lists
	if (monitor->enable_list_files) {
		log_name = string_format("%s.files", log_prefix);
		makeflow_hook_add_output_file(n->d, task, log_name, NULL, DAG_FILE_TYPE_INTERMEDIATE);
		free(log_name);
	}

	char *extra_options = string_format("-V '%s%s'", "category:", n->category->name);

	char *output_prefix = NULL;
	if (batch_queue_supports_feature(makeflow_get_queue(n), "output_directories")) {
		output_prefix = xxstrdup(log_prefix);
	} else {
		output_prefix = xxstrdup(path_basename(log_prefix));
	}

	char *cmd = resource_monitor_write_command(executable, 
					output_prefix, 
					dag_node_dynamic_label(n), 
					extra_options,
					monitor->enable_debug, 
					monitor->enable_time_series, 
					monitor->enable_list_files,
					monitor->measure_dir);

	free(executable);
	free(extra_options);
	free(output_prefix);
	free(log_prefix);

	batch_job_wrap_command(task, cmd);
	free(cmd);

	batch_wrapper_cmd(wrapper, task->command);

	cmd = batch_wrapper_write(wrapper, task);
	if (cmd) {
		batch_job_set_command(task, cmd);
		struct dag_file *df = makeflow_hook_add_input_file(n->d, task, cmd, cmd, DAG_FILE_TYPE_TEMP);
		debug(D_MAKEFLOW_HOOK, "Wrapper written to %s", df->filename);
		makeflow_log_file_state_change(n->d, df, DAG_FILE_STATE_EXISTS);
	} else {
		debug(D_MAKEFLOW_HOOK, "Failed to create wrapper: errno %d, %s", errno, strerror(errno));
		return MAKEFLOW_HOOK_FAILURE;
	}
	free(cmd);

	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_monitor_move_output_if_needed(struct makeflow_monitor *monitor, struct dag_node *n, struct batch_queue *queue)
{
	if (!batch_queue_supports_feature(queue, "output_directories")) {
		struct dag_file *f;
		char *log_prefix = set_log_prefix(monitor, n);
		char *output_prefix = xxstrdup(path_basename(log_prefix));

		if (!strcmp(log_prefix, output_prefix)) { // They are in the same location so no move
			free(log_prefix);
			free(output_prefix);
			return MAKEFLOW_HOOK_SUCCESS;
		}

		char *old_path = string_format("%s.summary", output_prefix);
		char *new_path = string_format("%s.summary", log_prefix);
		if (rename(old_path, new_path) == -1) {
			debug(D_ERROR|D_MAKEFLOW_HOOK, "Error moving Resource Monitor output %s:%s. %s\n", old_path, new_path,
					strerror(errno));
			return MAKEFLOW_HOOK_FAILURE;
		} else {
			f = dag_file_from_name(n->d, old_path);
			if(f) makeflow_log_file_state_change(n->d, f, DAG_FILE_STATE_DELETE);
		}
		free(old_path);
		free(new_path);

		if (monitor->enable_time_series) {
			char *old_path = string_format("%s.series", output_prefix);
			char *new_path = string_format("%s.series", log_prefix);
			if (rename(old_path, new_path) == -1) {
				debug(D_ERROR|D_MAKEFLOW_HOOK, "Error moving Resource Monitor output %s:%s. %s\n", old_path,
						new_path, strerror(errno));
				return MAKEFLOW_HOOK_FAILURE;
			} else {
				f = dag_file_from_name(n->d, old_path);
				if(f) makeflow_log_file_state_change(n->d, f, DAG_FILE_STATE_DELETE);
			}
			free(old_path);
			free(new_path);
		}

		if (monitor->enable_list_files) {
			char *old_path = string_format("%s.files", output_prefix);
			char *new_path = string_format("%s.files", log_prefix);
			if (rename(old_path, new_path) == -1) {
				debug(D_ERROR|D_MAKEFLOW_HOOK, "Error moving Resource Monitor output %s:%s. %s\n", old_path,
						new_path, strerror(errno));
				return MAKEFLOW_HOOK_FAILURE;
			} else {
				f = dag_file_from_name(n->d, old_path);
				if(f) makeflow_log_file_state_change(n->d, f, DAG_FILE_STATE_DELETE);
			}
			free(old_path);
			free(new_path);
		}

		free(log_prefix);
		free(output_prefix);
	}
	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_end(void * instance_struct, struct dag_node *n, struct batch_job *task)
{
	struct makeflow_monitor *monitor = (struct makeflow_monitor*)instance_struct;
	char *log_prefix = set_log_prefix(monitor, n);
	char *output_prefix = NULL;
	if (batch_queue_supports_feature(makeflow_get_queue(n), "output_directories")) {
		output_prefix = xxstrdup(log_prefix);
	} else {
		output_prefix = xxstrdup(path_basename(log_prefix));
	}
	char *summary_name = string_format("%s.summary", output_prefix);

	if (n->resources_measured)
		rmsummary_delete(n->resources_measured);
	n->resources_measured = rmsummary_parse_file_single(summary_name);

	/* If the resources_measured is null, then the expected files to move
	 * are non-existent. This will cause the move function to fail and bail
	 * on the makeflow as a whole, which we don't want. */
	if(!n->resources_measured){
		debug(D_MAKEFLOW_HOOK, "Resource Monitor failed to measure resources.\n");
		return MAKEFLOW_HOOK_SUCCESS;
	}

	category_accumulate_summary(n->category, n->resources_measured, NULL);

	free(log_prefix);
	free(output_prefix);
	free(summary_name);

	return makeflow_monitor_move_output_if_needed(monitor, n, makeflow_get_queue(n));
}

static int node_fail(void * instance_struct, struct dag_node *n, struct batch_job *task)
{
	struct makeflow_monitor *monitor = (struct makeflow_monitor*)instance_struct;
	int rc = MAKEFLOW_HOOK_FAILURE;
	/* Currently checking the case where either rm ran out of disk or it caught an overflow. Slightly redundant. */
	if ((task->info->disk_allocation_exhausted) || (task->info->exit_code == RM_OVERFLOW)) {
		debug(D_MAKEFLOW_HOOK, "rule %d failed because it exceeded the resources limits.\n", n->nodeid);
		if (n->resources_measured && n->resources_measured->limits_exceeded) {
			char *str = rmsummary_print_string(n->resources_measured->limits_exceeded, 1);
			debug(D_MAKEFLOW_HOOK, "%s", str);
			free(str);
		}
	} else {
		debug(D_MAKEFLOW_HOOK, "rule %d failed, but was not attributed to resource monitor.\n", n->nodeid);
		/* The node failed, but we are not sure its the resource monitor.
		 * We will return that did not fail on resource monitor, but still
		 * update the next allocation numbers incase it was. */
		rc = MAKEFLOW_HOOK_SUCCESS;
	}

	if(monitor->instance == instances){
		category_allocation_t next = category_next_label(n->category, n->resource_request,
			/* resource overflow */ 1, n->resources_requested, n->resources_measured);

		if (next != CATEGORY_ALLOCATION_ERROR) {
			debug(D_MAKEFLOW_HOOK, "Rule %d resubmitted using new resource allocation.\n", n->nodeid);
			n->resource_request = next;
			makeflow_log_state_change(n->d, n, DAG_NODE_STATE_WAITING);
		} else {
			debug(D_MAKEFLOW_HOOK, "Rule %d failed to setting new resource allocation.\n", n->nodeid);
		}
	}
	return rc;
}

struct makeflow_hook makeflow_hook_resource_monitor = {
		.module_name = "Resource Monitor",

		.register_hook = resource_monitor_register_hook,
		.create = create,
		.destroy = destroy,

		.dag_start = dag_start,

		.node_submit = node_submit,
		.node_end = node_end,
		.node_fail = node_fail,
};
