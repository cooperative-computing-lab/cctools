#ifndef DS_REMOTE_FILE_INFO_H
#define DS_REMOTE_FILE_INFO_H

#include "dataswarm.h"

struct ds_remote_file_info {
	ds_file_t type;
	int64_t           size;
	time_t            mtime;
	timestamp_t       transfer_time;
};

struct ds_remote_file_info * ds_remote_file_info_create( ds_file_t type, int64_t size, time_t mtime );
void ds_remote_file_info_delete( struct ds_remote_file_info *rinfo );

#endif

