/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_REMOTE_FILE_TABLE_H
#define VINE_REMOTE_FILE_TABLE_H

#include "taskvine.h"

int vine_remote_file_table_insert(struct vine_worker_info *w, const char *cachename, struct vine_remote_file_info *remote_info);

struct vine_remote_file_info *vine_remote_file_table_remove(struct vine_worker_info *w, const char *cachename);

struct vine_remote_file_info *vine_remote_file_table_lookup(struct vine_worker_info *w, const char *cachename);

struct vine_worker_info *vine_remote_file_table_find_worker(struct vine_manager *q, const char *cachename);

#endif

