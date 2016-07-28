
/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
 */

#ifndef MAKEFLOW_WRAPPER_H
#define MAKEFLOW_WRAPPER_H

#define DEFAULT_MONITOR_LOG_FORMAT "resource-rule-%%"

typedef enum {
    CONTAINER_MODE_NONE,
    CONTAINER_MODE_DOCKER,
    CONTAINER_MODE_SINGULARITY,
    // CONTAINER_MODE_ROCKET etc
} container_mode_t;

/*
This module implements garbage collection on the dag.
Files that are no longer needed as inputs to any rules
may be removed, according to a variety of criteria.
 */

struct makeflow_wrapper {
    const char *command;
    struct list *input_files;
    struct list *output_files;

    struct itable *remote_names;
    struct hash_table *remote_names_inv;

    int uses_remote_rename;
};

struct makeflow_wrapper * makeflow_wrapper_create();
void makeflow_wrapper_add_command(struct makeflow_wrapper *w, const char *cmd);
void makeflow_wrapper_add_input_file(struct makeflow_wrapper *w, const char *file);
void makeflow_wrapper_add_output_file(struct makeflow_wrapper *w, const char *file);
struct list *makeflow_wrapper_generate_files(struct list *result, struct list *input, struct dag_node *n, struct makeflow_wrapper *w);
char *makeflow_wrap_wrapper(char *result, struct dag_node *n, struct makeflow_wrapper *w);
const char *makeflow_wrapper_get_remote_name(struct makeflow_wrapper *w, struct dag *d, const char *filename);

#endif
