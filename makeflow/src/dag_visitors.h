/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DAG_VISITORS_H
#define DAG_VISITORS_H

#include "dag.h"
#include "jx.h"

/* The dag_to_file function write a struct dag in memory to a
 * file, using the remotename names generated from
 * translate_filename, rather than the original filenames.
 */
int dag_to_file(const struct dag *d, const char *dag_file, char *(*rename)(struct dag_node *d, const char *filename));

/* The dag_to_dax function writes a struct dag in memory to file
 * using the DAX format
 */
int dag_to_dax(const struct dag *d, const char *name );

/* The dag_to_dot function writes a struct dag in memory to a dot
 * file (graphviz), giving the graphical presentation of the makeflow.
 */
void dag_to_dot(struct dag *d, int condense_display, int change_size, int with_labels, int task_id, int with_detail, char *graph_attr, char *node_attr, char *edge_attr, char *task_attr, char *file_attr );

/* The dag_to_ppm function writes a struct dag in memory to a ppm
 * file, giving a graphical presentation of the makeflow
 */
void dag_to_ppm(struct dag *d, int ppm_mode, char *ppm_option);

/* The dag_to_cyto function writes a struct dag in memory to a xgmml
 * file, giving a graphical presentation of the makeflow for use in Cytoscape
 */
void dag_to_cyto(struct dag *d, int condense_display, int change_size);

/* Generate a JSON representation of the given DAG.
 */
struct jx *dag_to_json(struct dag *d);

#endif
