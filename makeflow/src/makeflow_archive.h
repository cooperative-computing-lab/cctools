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

/* Given a node, generate the archive_id from the input files and command */
void makeflow_archive_node_generate_id(struct dag_node *n, char *command, struct list*inputs);

/* Preserves the current node within the archiving directory
   The source makeflow file, ancestor node archive_ids, and the output files are archived */
void makeflow_archive_populate(struct dag *d, struct dag_node *n, char *command, struct list *inputs, struct list *outputs, struct batch_job_info *info);

/* Returns true if a node has been preserved within the archiving directory*/
int makeflow_archive_is_preserved(struct dag *d, struct dag_node *n, char *command, struct list *inputs, struct list *outputs);

/* copies files from archiving directory to working directory */
int makeflow_archive_copy_preserved_files(struct dag *d, struct dag_node *n, struct list *outputs);

/* writes the run_info files that is stored within each archived node */
void makeflow_write_run_info(struct dag *d, struct dag_node *n, char *archive_path, struct batch_job_info *info, char *command);

/* writes the file symlink that links to the archived job that created it */
void makeflow_write_file_checksum(struct dag *d, struct dag_file *f, char *job_archive_path);

/* generates the checksum of a file's contents and stores it within the dag_file struct */
void generate_file_archive_id(struct dag_file *f);

/* writes a link from an ancestor node's to the current node */
void write_descendant_link(struct dag *d, struct dag_node *current_node, struct dag_node *ancestor_node);

/* writes a link from a the current node to an ancestor node */
void write_ancestor_links(struct dag *d, struct dag_node *current_node, struct dag_node *ancestor_node);

#endif
