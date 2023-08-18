/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_file_replica.h"

struct vine_file_replica *vine_file_replica_create(int64_t size, time_t mtime)
{
	struct vine_file_replica *rinfo = malloc(sizeof(*rinfo));
	rinfo->size = size;
	rinfo->mtime = mtime;
	rinfo->transfer_time = 0;
	rinfo->in_cache = 0;
	return rinfo;
}

void vine_file_replica_delete(struct vine_file_replica *rinfo) { free(rinfo); }
