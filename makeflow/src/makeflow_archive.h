/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_ARCHIVE_H
#define MAKEFLOW_ARCHIVE_H

#include "dag.h"
#include "dag_file.h"
#include "dag_node.h"
#include "sha1.h"
#include "list.h"

#define MAKEFLOW_ARCHIVE_DEFAULT_DIRECTORY "/tmp/makeflow.archive."

/* Preserves the current node within the archiving directory
   The source makeflow file, ancestor node archive_ids, and the output files are archived */
void makeflow_archive_populate(struct dag *d, struct dag_node *n, char *command, struct list *inputs, struct list *outputs, struct batch_job_info *info);

/* Returns true if a node has been preserved within the archiving directory*/
int makeflow_archive_is_preserved(struct dag *d, struct dag_node *n, char *command, struct list *inputs, struct list *outputs);

/* copies files from archiving directory to working directory */
int makeflow_archive_copy_preserved_files(struct dag *d, struct dag_node *n, struct list *outputs);

#endif
