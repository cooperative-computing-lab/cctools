/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_remote_file_info.h"

struct ds_remote_file_info * ds_remote_file_info_create( ds_file_t type, int64_t size, time_t mtime )
{
	struct ds_remote_file_info *rinfo = malloc(sizeof(*rinfo));
	rinfo->type = type;
	rinfo->size = size;
	rinfo->mtime = mtime;
	rinfo->transfer_time = 0;
	return rinfo;
}

void ds_remote_file_info_delete( struct ds_remote_file_info *rinfo )
{
	free(rinfo);
}

