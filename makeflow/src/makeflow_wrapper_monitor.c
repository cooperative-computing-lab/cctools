/*
 * Copyright (C) 2015- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

#include "rmonitor.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "dag.h"
#include "makeflow_wrapper.h"
#include "makeflow_wrapper_monitor.h"

#include <string.h>
#include <stdlib.h>

struct makeflow_monitor * makeflow_monitor_create()
{
	struct makeflow_monitor *m = malloc(sizeof(*m));
	m->wrapper = makeflow_wrapper_create();
	m->enable_debug       = 0;
	m->enable_time_series = 0;
	m->enable_list_files  = 0;

	m->limits_name	= NULL;
	m->interval		= 1;  // in seconds
	m->log_prefix	= NULL;
	m->exe			= NULL;
	m->exe_remote	= NULL;

	return m;
}

/*
 * Prepare for monitoring by creating wrapper command and attaching the
 * appropriate input and output dependencies.
 * */
void makeflow_prepare_for_monitoring( struct makeflow_monitor *m, char *log_dir, char *log_format)
{
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

char *makeflow_rmonitor_wrapper_command( struct makeflow_monitor *m, struct dag_node *n )
{
	char *executable;
	if(m->exe_remote && !n->local_job){
		executable = string_format("./%s", m->exe_remote);
	} else {
		executable = string_format("%s", m->exe);
	}
	char *limits_str = dag_node_resources_wrap_as_rmonitor_options(n);
	char *extra_options = string_format("%s -V '%-15s%s'",
			limits_str ? limits_str : "",
			"category:",
			n->category->label);

	char * result = resource_monitor_write_command(executable,
			m->log_prefix,
			m->limits_name,
			extra_options,
			m->enable_debug,
			m->enable_time_series,
			m->enable_list_files);

	char *nodeid = string_format("%d",n->nodeid);
	result = string_replace_percents(result, nodeid);

	free(executable);
	free(limits_str);
	free(extra_options);
	free(nodeid);

	return result;
}

/* Takes node->command and wraps it in wrapper_command. Then, if in monitor
 *  * mode, wraps the wrapped command in the monitor command. */
char *makeflow_wrap_monitor( char *result, struct dag_node *n, struct makeflow_monitor *m )
{
	if(!m) return result;

	char *monitor_command = makeflow_rmonitor_wrapper_command(m, n);
	result = string_wrap_command(result, monitor_command);
	free(monitor_command);

	return result;
}
