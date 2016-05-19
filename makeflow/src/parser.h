/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PARSER_H
#define PARSER_H

struct dag *dag_from_file(const char *filename);
void dag_close_over_categories(struct dag *d);
void dag_close_over_environment(struct dag *d);

#endif
