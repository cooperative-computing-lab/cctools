/*
 * Copyright (C) 2015- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

#include "rmonitor.h"
#include "stringtools.h"
#include "list.h"
#include "xxmalloc.h"

#include "dag.h"
#include "makeflow_wrapper.h"

#include <string.h>

/* Monitor control functions for wrapping command and adding input/output
 *  * files. */
char *monitor_exe  = "resource_monitor_cctools";

struct makeflow_wrapper * makeflow_wrapper_create()
{
	struct makeflow_wrapper *w = malloc(sizeof(*w));
	w->command = NULL;
	w->input_files = list_create();
	w->output_files = list_create();

	return w;
}

struct makeflow_monitor * makeflow_monitor_create()
{
	struct makeflow_monitor *m = malloc(sizeof(*m));
	m->wrapper = makeflow_wrapper_create();
	m->enable_time_series = 0;
	m->enable_list_files  = 0;

	m->limits_name	= NULL;
	m->interval		= 1;  // in seconds
	m->log_format	= xxstrdup(DEFAULT_MONITOR_LOG_FORMAT);
	m->log_dir		= NULL;
	m->exe			= NULL;

	return m;
}

void makeflow_wrapper_add_command( struct makeflow_wrapper *w, const char *cmd )
{
	if(!w->command) {
		w->command = strdup(cmd);
	} else {
		w->command = string_wrap_command(w->command,cmd);
	}
}

void makeflow_wrapper_add_input_file( struct makeflow_wrapper *w, const char *file )
{
	if(!w->input_files) w->input_files = list_create();
	char *f      = string_format("%s",file);
	list_push_tail(w->input_files, f);
}

void makeflow_wrapper_add_output_file( struct makeflow_wrapper *w, const char *file )
{
	if(!w->output_files) w->output_files = list_create();
	char *f      = string_format("%s",file);
	list_push_tail(w->output_files, f);
}

struct list *makeflow_wrapper_generate_files( struct list *result, struct list *input, struct dag_node *n )
{
	char *f;
	char *nodeid = string_format("%d",n->nodeid);

	struct list *files = list_create();

	list_first_item(input);
	while((f = list_next_item(input)))
	{
		char *filename = strdup(f);
		filename = string_replace_percents(filename, nodeid);
		struct dag_file *file = dag_file_lookup_or_create(n->d, filename);
		list_push_tail(files, file);
	}
	free(nodeid);

	result = list_splice(result, files);

	return result;
}

/*
 * Prepare for monitoring by creating wrapper command and attaching the
 * appropriate input and output dependencies.
 * */
void makeflow_prepare_for_monitoring( struct makeflow_monitor *m)
{
	char *log_prefix = string_format("%s/%s", m->log_dir, m->log_format);
	char *log_name;

	makeflow_wrapper_add_input_file(m->wrapper, m->exe);

	log_name = string_format("%s.summary", log_prefix);
	makeflow_wrapper_add_output_file(m->wrapper, log_name);
	free(log_name);

	if(m->enable_time_series)
	{
		log_name = string_format("%s.series", log_prefix);
		makeflow_wrapper_add_output_file(m->wrapper, log_name);
		free(log_name);
	}

	if(m->enable_list_files)
	{
		log_name = string_format("%s.files", log_prefix);
		makeflow_wrapper_add_output_file(m->wrapper, log_name);
		free(log_name);
	}

	free(log_prefix);
}

/*
 * Creates a wrapper command with the appropriate resource monitor string for a given node.
 * Returns a newly allocated string that must be freed.
 * */

char *makeflow_rmonitor_wrapper_command( struct makeflow_monitor *m, struct dag_node *n )
{
	char *log_prefix = string_format("%s/%s", m->log_dir, m->log_format);
	char *limits_str = dag_node_resources_wrap_as_rmonitor_options(n);
	char *extra_options = string_format("%s -V '%-15s%s'",
			limits_str ? limits_str : "",
			"category:",
			n->category->label);

	char * result = resource_monitor_write_command(m->exe,
			log_prefix,
			m->limits_name,
			extra_options,
			m->enable_time_series,
			m->enable_list_files);

	char *nodeid = string_format("%d",n->nodeid);
	result = string_replace_percents(result, nodeid);

	free(log_prefix);
	free(limits_str);
	free(extra_options);
	free(nodeid);

	return result;
}

/* Takes node->command and wraps it in wrapper_command. Then, if in monitor
 *  * mode, wraps the wrapped command in the monitor command. */
char *makeflow_wrap_wrapper( char *result,  struct dag_node *n, struct makeflow_wrapper *w )
{
	if(!w) return result;

	char *nodeid = string_format("%d",n->nodeid);
	char *wrap_tmp = strdup(w->command);
	wrap_tmp = string_replace_percents(wrap_tmp, nodeid);

	free(nodeid);

	result = string_wrap_command(result, wrap_tmp);
	free(wrap_tmp);

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


/* 1) create a global script for running docker container
 * 2) add this script to the global wrapper list
 * 3) reformat each task command
 */
void makeflow_wrapper_docker_init( struct makeflow_wrapper *w, char *container_image, char *image_tar )
{
	FILE *wrapper_fn;

	wrapper_fn = fopen(CONTAINER_SH, "w");

	if (image_tar == NULL) {

		fprintf(wrapper_fn, "#!/bin/sh\n\
curr_dir=`pwd`\n\
default_dir=/root/worker\n\
flock /tmp/lockfile /usr/bin/docker pull %s\n\
docker run --rm -m 1g -v $curr_dir:$default_dir -w $default_dir %s \"$@\"\n", container_image, container_image);

	} else {

		fprintf(wrapper_fn, "#!/bin/sh\n\
curr_dir=`pwd`\n\
default_dir=/root/worker\n\
flock /tmp/lockfile /usr/bin/docker load < %s\n\
docker run --rm -m 1g -v $curr_dir:$default_dir -w $default_dir %s \"$@\"\n", image_tar, container_image);

		makeflow_wrapper_add_input_file(w, image_tar);
	}

	fclose(wrapper_fn);

	chmod(CONTAINER_SH, 0755);

	makeflow_wrapper_add_input_file(w, CONTAINER_SH);

	char *global_cmd = string_format("sh %s", CONTAINER_SH);
	makeflow_wrapper_add_command(w, global_cmd);
}
