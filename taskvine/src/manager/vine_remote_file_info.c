/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_remote_file_info.h"

struct vine_remote_file_info * vine_remote_file_info_create( vine_file_t type, int64_t size, time_t mtime )
{
	struct vine_remote_file_info *rinfo = malloc(sizeof(*rinfo));
	rinfo->type = type;
	rinfo->size = size;
	rinfo->mtime = mtime;
	rinfo->transfer_time = 0;
	rinfo->in_cache = 0;
	return rinfo;
}

void vine_remote_file_info_delete( struct vine_remote_file_info *rinfo )
{
	free(rinfo);
}

