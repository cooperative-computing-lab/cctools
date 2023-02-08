/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PARSER_H
#define PARSER_H

/* the type of a dependency specified in the mountfile */
typedef enum {
	DAG_SYNTAX_MAKE = 1,
	DAG_SYNTAX_JSON,
	DAG_SYNTAX_JX
} dag_syntax_type;


struct dag *dag_from_file(const char *filename, dag_syntax_type format, struct jx *args);
void dag_close_over_nodes(struct dag *d);
void dag_close_over_categories(struct dag *d);
void dag_close_over_environment(struct dag *d);

#endif
