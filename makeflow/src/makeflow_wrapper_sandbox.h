
/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_WRAPPER_SANDBOX_H
#define MAKEFLOW_WRAPPER_SANDBOX_H

void makeflow_wrapper_sandbox_init( struct makeflow_wrapper *w, char *parrot_path );
char *makeflow_wrap_sandbox( char *result, struct dag_node *n, struct makeflow_wrapper *w, struct list *input_list, struct list *output_list );

#endif
