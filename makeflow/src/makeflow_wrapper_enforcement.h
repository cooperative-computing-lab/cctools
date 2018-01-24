
/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_WRAPPER_ENFORCEMENT_H
#define MAKEFLOW_WRAPPER_ENFORCEMENT_H

void makeflow_wrapper_enforcer_init( struct makeflow_wrapper *w, char *parrot_path );
void makeflow_wrap_enforcer( struct batch_task *task, struct dag_node *n, struct makeflow_wrapper *w);

#endif
