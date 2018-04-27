/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DAG_NODE_HASH_H
#define DAG_NODE_HASH_H

#include "sha1.h"

struct dag_node_hash *dag_node_hash_create(void);

void dag_node_hash_command(struct dag_node_hash *h, const char *cmd);
void dag_node_hash_makeflow(struct dag_node_hash *h, const char *dag, const char *cwd);
void dag_node_hash_source(struct dag_node_hash *h, const char *src);
void dag_node_hash_target(struct dag_node_hash *h, const char *tgt);

// frees h
void dag_node_hash(struct dag_node_hash *h, unsigned char digest[SHA1_DIGEST_LENGTH]);

#endif
