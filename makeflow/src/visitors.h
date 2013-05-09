/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


#include "dag.h"

/* The dag_to_file function write a struct dag in memory to a
 * file, using the remotename names generated from
 * translate_filename, rather than the original filenames.
 */
int dag_to_file(const struct dag *d, const char *dag_file, char *(*rename)(struct dag_node *d, const char *filename));


/* The dag_to_dot function writes a struct dag in memory to a dot
 * file (graphviz), giving the graphical presentation of the makeflow.
 */
void dag_to_dot(struct dag *d, int condense_display, int change_size);

/* The dag_to_ppm function writes a struct dag in memory to a ppm
 * file, giving a graphical presentation of the makeflow
 */
void dag_to_ppm(struct dag *d, int ppm_mode, char *ppm_option);
