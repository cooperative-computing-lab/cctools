
/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_WRAPPER_H
#define MAKEFLOW_WRAPPER_H

#define DEFAULT_MONITOR_LOG_FORMAT "resource-rule-%%"

#define CONTAINER_SH "docker.wrapper.sh"

/*
This module implements garbage collection on the dag.
Files that are no longer needed as inputs to any rules
may be removed, according to a variety of criteria.
*/


typedef enum {
	CONTAINER_MODE_NONE,
	CONTAINER_MODE_DOCKER,
	// CONTAINER_MODE_ROCKET etc
} container_mode_t;

struct makeflow_wrapper {
	const char *command;
	struct list *input_files;
	struct list *output_files;

	struct itable *remote_names;
	struct hash_table *remote_names_inv;
};

struct makeflow_monitor {
	struct makeflow_wrapper *wrapper;
	int enable_time_series;
	int enable_list_files;

	int interval;
	char *log_prefix;
	char *limits_name;
	char *exe;
	const char *exe_remote;
};


struct makeflow_wrapper * makeflow_wrapper_create();
void makeflow_wrapper_add_command( struct makeflow_wrapper *w, const char *cmd );
void makeflow_wrapper_add_input_file( struct makeflow_wrapper *w, const char *file );
void makeflow_wrapper_add_output_file( struct makeflow_wrapper *w, const char *file );
struct list *makeflow_wrapper_generate_files( struct list *result, struct list *input, struct dag_node *n , struct makeflow_wrapper *w );
char *makeflow_wrap_wrapper( char *result, struct dag_node *n, struct makeflow_wrapper *w );
const char *makeflow_wrapper_get_remote_name(struct makeflow_wrapper *w, struct dag *d, const char *filename);

struct makeflow_monitor * makeflow_monitor_create();
void makeflow_prepare_for_monitoring( struct makeflow_monitor *m, char *log_dir, char *log_format);
char *makeflow_wrap_monitor( char *result, struct dag_node *n, struct makeflow_monitor *m );

void makeflow_wrapper_docker_init( struct makeflow_wrapper *w, char *container_image, char *image_tar );

#endif
