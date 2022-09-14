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
	ds_file_t type;         // Type of data source: DS_FILE, DS_BUFFER, DS_URL, etc.
	ds_file_flags_t flags;	// Special handling: DS_CACHE for caching, DS_WATCH for watching, etc.
	int length;		// Length of source data, if known.
	off_t offset;		// File offset for DS_FILE_PIECE
	off_t piece_length;	// File piece length for DS_FILE_PIECE
	char *source;		// Name of source file, url, or literal data if a DS_BUFFER.
	char *remote_name;	// Name of file as it appears to the task.
	char *cached_name;	// Name of file in the worker's cache directory.
};

struct ds_file * ds_file_create( const char *source, const char *remote_name, ds_file_t type, ds_file_flags_t flags );
struct ds_file *ds_file_clone( const struct ds_file *file );
void ds_file_delete( struct ds_file *f );

#endif
