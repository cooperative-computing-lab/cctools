/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


/*
 * Abstract the files posessed by the workers to a single "table"
 */


#ifndef VINE_FILE_REPLICA_TABLE_H
#define VINE_FILE_REPLICA_TABLE_H

#include "taskvine.h"
#include "vine_file_replica.h"
#include "vine_worker_info.h"

int vine_file_replica_table_insert(struct vine_worker_info *w, const char *cachename, struct vine_file_replica *remote_info);

struct vine_file_replica *vine_file_replica_table_remove(struct vine_worker_info *w, const char *cachename);

struct vine_file_replica *vine_file_replica_table_lookup(struct vine_worker_info *w, const char *cachename);

struct vine_worker_info *vine_file_replica_table_find_worker(struct vine_manager *q, const char *cachename);

int vine_file_replica_table_exists_somewhere( struct vine_manager *q, const char *cachename );


#endif

