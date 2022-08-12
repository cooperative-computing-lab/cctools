/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_json.h"
#include "ds_file.h"
#include "ds_task.h"

#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *ds_properties[] = { "name", "port", "priority", "num_tasks_left", "next_taskid", "workingdir", "manager_link",
	"poll_table", "poll_table_size", "tasks", "task_state_map", "ready_list", "worker_table",
	"worker_blacklist", "worker_task_map", "categories", "workers_with_available_results",
	"stats", "stats_measure", "stats_disconnected_workers", "time_last_wait",
	"worker_selection_algorithm", "task_ordering", "process_pending_check", "short_timeout",
	"long_timeout", "task_reports", "asynchrony_multiplier", "asynchrony_modifier",
	"minimum_transfer_timeout", "foreman_transfer_timeout", "transfer_outlier_factor",
	"default_transfer_rate", "catalog_hosts", "catalog_last_update_time",
	"resources_last_update_time", "busy_waiting_flag", "allocation_default_mode", "logfile",
	"transactions_logfile", "keepalive_interval", "keepalive_timeout", "link_poll_end",
	"manager_preferred_connection", "monitor_mode", "monitor_file", "monitor_output_directory",
	"monitor_summary_filename", "monitor_exe", "measured_local_resources",
	"current_max_worker", "password", "bandwidth", NULL
};

static const char *ds_task_properties[] = { "tag", "command_line", "worker_selection_algorithm", "output", "input_files", "environment",
	"output_files", "env_list", "taskid", "return_status", "result", "host", "hostname",
	"category", "resource_request", "priority", "max_retries", "try_count",
	"exhausted_attempts", "time_when_submitted", "time_when_done",
	"disk_allocation_exhausted", "time_when_commit_start", "time_when_commit_end",
	"time_when_retrieval", "time_workers_execute_last", "time_workers_execute_all",
	"time_workers_execute_exhaustion", "time_workers_execute_failure", "bytes_received",
	"bytes_sent", "bytes_transferred", "resources_allocated", "resources_measured",
	"resources_requested", "monitor_output_directory", "monitor_snapshot_file", "features",
	"time_task_submit", "time_task_finish", "time_committed", "time_send_input_start",
	"time_send_input_finish", "time_receive_result_start", "time_receive_result_finish",
	"time_receive_output_start", "time_receive_output_finish", "time_execute_cmd_start",
	"time_execute_cmd_finish", "total_transfer_time", "cmd_execution_time",
	"total_cmd_execution_time", "total_cmd_exhausted_execute_time",
	"total_time_until_worker_failure", "total_bytes_received", "total_bytes_sent",
	"total_bytes_transferred", "time_app_delay", "cores", "memory", "disk", NULL
};


static int is_in(const char *str, const char **array)
{

	const char **ptr = array;

	while(*ptr != 0) {

		if(!strcmp(*ptr, str)) {
			return 1;
		}

		ptr++;

	}

	return 0;

}

static int validate_json(struct jx *json, const char **array)
{

	//iterate over the keys in a JX_OBJECT
	void *j = NULL;
	const char *key = jx_iterate_keys(json, &j);

	while(key != NULL) {

		if(!is_in(key, array)) {
			return 1;
		}

		key = jx_iterate_keys(json, &j);

	}

	return 0;

}

static int specify_files(int input, struct jx *files, struct ds_task *task)
{

	void *i = NULL;
	struct jx *arr = jx_iterate_array(files, &i);

	while(arr != NULL) {

		char *local = NULL;
		char *remote = NULL;
		struct jx_pair *flag;
		void *k = NULL;
		void *v = NULL;
		int flags = 0;

		const char *key = jx_iterate_keys(arr, &k);
		struct jx *value = jx_iterate_values(arr, &v);

		while(key != NULL) {


			if(!strcmp(key, "local_name")) {
				local = value->u.string_value;
			} else if(!strcmp(key, "remote_name")) {
				remote = value->u.string_value;
			} else if(!strcmp(key, "flags")) {
				flag = value->u.pairs;
				while(flag) {
					char *flag_key = flag->key->u.string_value;
					bool flag_value = flag->value->u.boolean_value;
					if(!strcmp(flag_key, "cache")) {
						if(flag_value) {
							flags |= DS_CACHE;
						}
					} else if(!strcmp(flag_key, "watch")) {
						if(flag_value) {
							flags |= DS_WATCH;
						}
					} else {
						printf("KEY ERROR: %s not valid\n", flag_key);
						return 1;
					}
					flag = flag->next;
				}
			} else {
				printf("KEY ERROR: %s not valid\n", key);
				return 1;
			}

			key = jx_iterate_keys(arr, &k);
			value = jx_iterate_values(arr, &v);

		}

		if(input) {
			ds_task_specify_file(task, local, remote, DS_INPUT, flags);
		} else {
			ds_task_specify_file(task, local, remote, DS_OUTPUT, flags);
		}

		arr = jx_iterate_array(files, &i);

	}

	return 0;

}

static int specify_environment(struct jx *environment, struct ds_task *task)
{
	void *j = NULL;
	void *i = NULL;
	const char *key = jx_iterate_keys(environment, &j);
	struct jx *value = jx_iterate_values(environment, &i);

	while(key != NULL) {
		ds_task_specify_environment_variable(task, key, value->u.string_value);
		key = jx_iterate_keys(environment, &j);
		value = jx_iterate_values(environment, &i);
	}

	return 0;
}



static struct ds_task *create_task(const char *str)
{

	char *command_line = NULL;
	struct jx *input_files = NULL;
	struct jx *output_files = NULL;
	struct jx *environment = NULL;
	int cores = 0, memory = 0, disk = 0;

	struct jx *json = jx_parse_string(str);
	if(!json) {
		return NULL;
	}
	//validate json
	if(validate_json(json, ds_task_properties)) {
		return NULL;
	}
	//get command from json
	void *j = NULL;
	void *i = NULL;
	const char *key = jx_iterate_keys(json, &j);
	struct jx *value = jx_iterate_values(json, &i);

	while(key != NULL) {

		if(!strcmp(key, "command_line")) {
			command_line = value->u.string_value;
		} else if(!strcmp(key, "input_files")) {
			input_files = value;
		} else if(!strcmp(key, "output_files")) {
			output_files = value;
		} else if(!strcmp(key, "environment")) {
			environment = value;
		} else if(!strcmp(key, "cores")) {
			cores = value->u.integer_value;
		} else if(!strcmp(key, "memory")) {
			memory = value->u.integer_value;
		} else if(!strcmp(key, "disk")) {
			disk = value->u.integer_value;
		} else {
			printf("%s\n", key);
		}

		key = jx_iterate_keys(json, &j);
		value = jx_iterate_values(json, &i);

	}

	//call ds_task_create
	if(command_line) {

		struct ds_task *task = ds_task_create(command_line);

		if(!task) {
			return NULL;
		}

		if(input_files) {
			specify_files(1, input_files, task);
		}

		if(output_files) {
			specify_files(0, output_files, task);
		}

		if(environment) {
			specify_environment(environment, task);
		}

		if(cores) {
			ds_task_specify_cores(task, cores);
		}

		if(memory) {
			ds_task_specify_memory(task, memory);
		}

		if(disk) {
			ds_task_specify_disk(task, disk);
		}
		return task;

	}

	return NULL;

}

struct ds_manager *ds_json_create(const char *str)
{


	int port = -1, priority = 0;
	char *name = NULL;

	struct jx *json = jx_parse_string(str);
	if(!json) {
		return NULL;
	}
	//validate json
	if(validate_json(json, ds_properties)) {
		return NULL;
	}

	void *i = NULL;
	void *j = NULL;
	const char *key = jx_iterate_keys(json, &j);
	struct jx *value = jx_iterate_values(json, &i);

	while(key != NULL) {

		if(!strcmp(key, "name")) {
			name = value->u.string_value;
		} else if(!strcmp(key, "port")) {
			port = value->u.integer_value;
		} else if(!strcmp(key, "priority")) {
			priority = value->u.integer_value;
		} else {
			printf("Not necessary: %s\n", key);
		}

		key = jx_iterate_keys(json, &j);
		value = jx_iterate_values(json, &i);

	}

	if(port >= 0) {

		struct ds_manager *dataswarm = ds_create(port);

		if(!dataswarm) {
			return NULL;
		}

		if(name) {
			ds_specify_name(dataswarm, name);
		}
		if(priority) {
			ds_specify_priority(dataswarm, priority);
		}

		return dataswarm;

	}

	return NULL;

}

int ds_json_submit(struct ds_manager *q, const char *str)
{

	struct ds_task *task;

	task = create_task(str);

	if(task) {
		return ds_submit(q, task);
	} else {
		return -1;
	}

}

char *ds_json_wait(struct ds_manager *q, int timeout)
{

	char *task;
	struct jx *j;
	struct jx_pair *command_line, *taskid, *return_status, *output, *result;

	struct ds_task *t = ds_wait(q, timeout);

	if(!t) {
		return NULL;
	}

	command_line = jx_pair(jx_string("command_line"), jx_string(t->command_line), NULL);
	taskid = jx_pair(jx_string("taskid"), jx_integer(t->taskid), command_line);
	return_status = jx_pair(jx_string("return_status"), jx_integer(t->return_status), taskid);
	result = jx_pair(jx_string("result"), jx_integer(t->result), return_status);

	if(t->output) {
		output = jx_pair(jx_string("output"), jx_string(t->output), result);
	} else {
		output = jx_pair(jx_string("output"), jx_string(""), result);
	}

	j = jx_object(output);

	task = jx_print_string(j);

	return task;
}

char *ds_json_remove(struct ds_manager *q, int id)
{

	char *task;
	struct jx *j;
	struct jx_pair *command_line, *taskid;

	struct ds_task *t = ds_cancel_by_taskid(q, id);

	if(!t) {
		return NULL;
	}

	command_line = jx_pair(jx_string("command_line"), jx_string(t->command_line), NULL);
	taskid = jx_pair(jx_string("taskid"), jx_integer(t->taskid), command_line);

	j = jx_object(taskid);

	task = jx_print_string(j);

	return task;

}

char *ds_json_get_status(struct ds_manager *q)
{
	char *status;
	struct ds_stats s;
	struct jx *j;
	struct jx_pair *workers_connected, *workers_idle, *workers_busy, *tasks_waiting, *tasks_on_workers, *tasks_running, *tasks_with_results, *tasks_submitted, *tasks_done, *tasks_failed, *bytes_sent, *bytes_received;

	ds_get_stats(q, &s);

	workers_connected = jx_pair(jx_string("workers_connected"), jx_integer(s.workers_connected), NULL);
	workers_idle = jx_pair(jx_string("workers_idle"), jx_integer(s.workers_idle), workers_connected);
	workers_busy = jx_pair(jx_string("workers_busy"), jx_integer(s.workers_busy), workers_idle);
	tasks_waiting = jx_pair(jx_string("tasks_waiting"), jx_integer(s.tasks_waiting), workers_busy);
	tasks_on_workers = jx_pair(jx_string("tasks_on_workers"), jx_integer(s.tasks_on_workers), tasks_waiting);
	tasks_running = jx_pair(jx_string("tasks_running"), jx_integer(s.tasks_running), tasks_on_workers);
	tasks_with_results = jx_pair(jx_string("tasks_with_results"), jx_integer(s.tasks_with_results), tasks_running);
	tasks_submitted = jx_pair(jx_string("tasks_submitted"), jx_integer(s.tasks_submitted), tasks_with_results);
	tasks_done = jx_pair(jx_string("tasks_done"), jx_integer(s.tasks_done), tasks_submitted);
	tasks_failed = jx_pair(jx_string("tasks_failed"), jx_integer(s.tasks_failed), tasks_done);
	bytes_sent = jx_pair(jx_string("bytes_sent"), jx_integer(s.bytes_sent), tasks_failed);
	bytes_received = jx_pair(jx_string("bytes_received"), jx_integer(s.bytes_received), bytes_sent);

	j = jx_object(bytes_received);

	status = jx_print_string(j);

	return status;

}
