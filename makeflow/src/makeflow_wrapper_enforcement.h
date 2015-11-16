
/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_WRAPPER_ENFORCEMENT_H
#define MAKEFLOW_WRAPPER_ENFORCEMENT_H

void makeflow_wrapper_enforcer_init( struct makeflow_wrapper *w, char *parrot_path );
char *makeflow_wrap_enforcer( char *result, struct dag_node *n, struct makeflow_wrapper *w, struct list *input_list, struct list *output_list );
struct list *makeflow_enforcer_generate_files( struct list *result, struct list *input, struct dag_node *n, struct makeflow_wrapper *w);

#endif
