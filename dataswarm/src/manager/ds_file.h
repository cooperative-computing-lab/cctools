/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_FILE_H
#define DS_FILE_H

#include "dataswarm.h"

#include <sys/types.h>

struct ds_file {
	ds_file_t type;
	ds_file_flags_t flags;	// DS_CACHE or others in the future.
	int length;		// length of payload, only used for non-file objects like buffers and urls
	off_t offset;		// file offset for DS_FILE_PIECE
	off_t piece_length;	// file piece length for DS_FILE_PIECE
	char *payload;		// name on master machine or buffer of data.
	char *remote_name;	// name on remote machine.
	char *cached_name;	// name on remote machine in cached directory.
};

struct ds_file * ds_file_create( const char *source, const char *remote_name, ds_file_t type, ds_file_flags_t flags );
struct ds_file *ds_file_clone( const struct ds_file *file );
void ds_file_delete( struct ds_file *f );

#endif
