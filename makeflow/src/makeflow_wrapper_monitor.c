/*
 * Copyright (C) 2015- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

#include "create_dir.h"
#include "debug.h"
#include "path.h"
#include "rmonitor.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "dag.h"
#include "dag_file.h"
#include "makeflow_log.h"
#include "makeflow_wrapper.h"
#include "makeflow_wrapper_monitor.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

struct makeflow_monitor * makeflow_monitor_create()
{
	struct makeflow_monitor *m = malloc(sizeof(*m));
	m->wrapper = makeflow_wrapper_create();
	m->enable_debug       = 0;
	m->enable_time_series = 0;
	m->enable_list_files  = 0;

	m->interval     = 1;  // in seconds
	m->log_prefix   = NULL;
	m->exe          = NULL;
	m->exe_remote	= NULL;

	return m;
}

void makeflow_monitor_delete(struct makeflow_monitor *m)
{
	makeflow_wrapper_delete(m->wrapper);
	if(m->log_prefix)
		free(m->log_prefix);

	if(m->exe)
		free(m->exe);

	free(m);
}

/*
 * Prepare for monitoring by creating wrapper command and attaching the
 * appropriate input and output dependencies.
 * */
void makeflow_prepare_for_monitoring( struct dag *d, struct makeflow_monitor *m, struct batch_queue *queue, char *log_dir, char *log_format)
{
	m->exe = resource_monitor_locate(NULL);
	if(!m->exe) {
		fatal("Monitor mode was enabled, but could not find resource_monitor in PATH.");
	}

	if(batch_queue_supports_feature(queue, "remote_rename")) {
		m->exe_remote = path_basename(m->exe);
	} else {
		m->exe_remote = NULL;
	}

	int result = mkdir(log_dir, 0777);
	if(result == -1){
		if(errno == ENOENT){
			result = !create_dir(log_dir, 0777);
		} else if(errno != EEXIST){
			fatal("Monitor mode was enabled, but could not created output directory. %s", strerror(errno));	
		}
	}
	if(result == 0){ // Either the mkdir was successful, or create_dir was successful. aka created in Makeflow
		struct dag_file *f = dag_file_lookup_or_create(d, log_dir);
		makeflow_log_file_state_change(d, f, DAG_FILE_STATE_EXISTS);
	}

	m->log_prefix = string_format("%s/%s", log_dir, log_format);
	char *log_name;

	if(m->exe_remote){
		log_name = string_format("%s=%s", m->exe, m->exe_remote);
		makeflow_wrapper_add_input_file(m->wrapper, log_name);
		free(log_name);
	} else {
		makeflow_wrapper_add_input_file(m->wrapper, m->exe);
	}

	log_name = string_format("%s.summary", m->log_prefix);
	makeflow_wrapper_add_output_file(m->wrapper, log_name);
	free(log_name);

	if(m->enable_time_series)
	{
		log_name = string_format("%s.series", m->log_prefix);
		makeflow_wrapper_add_output_file(m->wrapper, log_name);
		free(log_name);
	}

	if(m->enable_list_files)
	{
		log_name = string_format("%s.files", m->log_prefix);
		makeflow_wrapper_add_output_file(m->wrapper, log_name);
		free(log_name);
	}
}

/*
 * Creates a wrapper command with the appropriate resource monitor string for a given node.
 * Returns a newly allocated string that must be freed.
 * */

char *makeflow_rmonitor_wrapper_command( struct makeflow_monitor *m, struct batch_queue *queue, struct dag_node *n )
{
	char *executable;
	if(m->exe_remote && !n->local_job){
		executable = string_format("./%s", m->exe_remote);
	} else {
		executable = string_format("%s", m->exe);
	}
	char *extra_options = string_format("-V '%s%s'", "category:", n->category->name);

	char *output_prefix = NULL;
	if(batch_queue_supports_feature(queue, "output_directories")) {
		output_prefix = xxstrdup(m->log_prefix);
	} else {
		output_prefix = xxstrdup(path_basename(m->log_prefix));
	}

	char * result = resource_monitor_write_command(executable,
			output_prefix,
			dag_node_dynamic_label(n),
			extra_options,
			m->enable_debug,
			m->enable_time_series,
			m->enable_list_files);

	char *nodeid = string_format("%d",n->nodeid);
	char *result = string_replace_percents(command, nodeid);

	free(executable);
	free(extra_options);
	free(nodeid);
	free(output_prefix);

	return result;
}

/* Takes node->command and wraps it in wrapper_command. Then, if in monitor
 *  * mode, wraps the wrapped command in the monitor command. */
char *makeflow_wrap_monitor( char *result, struct dag_node *n, struct batch_queue *queue, struct makeflow_monitor *m )
{
	if(!m) return result;

	char *monitor_command = makeflow_rmonitor_wrapper_command(m, queue, n);
	result = string_wrap_command(result, monitor_command);
	free(monitor_command);

	return result;
}

int makeflow_monitor_move_output_if_needed(struct dag_node *n, struct batch_queue *queue, struct makeflow_monitor *m)
{
	if(!batch_queue_supports_feature(queue, "output_directories")) {

		char *nodeid = string_format("%d",n->nodeid);
		char *log_prefix = string_replace_percents(m->log_prefix, nodeid);


		char *output_prefix = xxstrdup(path_basename(log_prefix));

		if(!strcmp(log_prefix, output_prefix)) // They are in the same location so no move
			return 0;

		char *old_path = string_format("%s.summary", output_prefix);
		char *new_path = string_format("%s.summary", log_prefix);
		if(rename(old_path, new_path)==-1){
			debug(D_MAKEFLOW_RUN, "Error moving Resource Monitor output %s:%s. %s\n",old_path, new_path, strerror(errno));
			return 1;
		}
		free(old_path);
		free(new_path);
	
		if(m->enable_time_series)
		{
			char *old_path = string_format("%s.series", output_prefix);
			char *new_path = string_format("%s.series", log_prefix);
			if(rename(old_path, new_path)==-1){
				debug(D_MAKEFLOW_RUN, "Error moving Resource Monitor output %s:%s. %s\n",old_path, new_path, strerror(errno));
				return 1;
			}
			free(old_path);
			free(new_path);
		}
	
		if(m->enable_list_files)
		{
			char *old_path = string_format("%s.files", output_prefix);
			char *new_path = string_format("%s.files", log_prefix);
			if(rename(old_path, new_path)==-1){
				debug(D_MAKEFLOW_RUN, "Error moving Resource Monitor output %s:%s. %s\n",old_path, new_path, strerror(errno));
				return 1;
			}
			free(old_path);
			free(new_path);
		}
	
		free(log_prefix);
		free(output_prefix);
	}	
	return 0;
}
