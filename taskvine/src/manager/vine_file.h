/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_FILE_H
#define VINE_FILE_H

/*
This module defines the internal structure and details of a single file.
Here, a "file" can come from many different sources: a local file,
a remote url, a command to run on the worker, etc, and is then eventually
mapped into a tasks' working directory.  As a result, it has several kinds of names:

- f->source indicates the name of the source file, url, or command that provides the data.
- f->cached_name indicates the name of the file as it is stored in the worker's cache.
- f->remote_name indicates the name of the file as the task expects to see it.

This module is private to the manager and should not be invoked by the end user.
*/

#include "taskvine.h"

#include <sys/types.h>

struct vine_file {
	vine_file_t type;         // Type of data source: VINE_FILE, VINE_BUFFER, VINE_URL, etc.
	vine_file_flags_t flags;	// Special handling: VINE_CACHE for caching, VINE_WATCH for watching, etc.
	int length;		// Length of source data, if known.
	off_t offset;		// File offset for VINE_FILE_PIECE
	off_t piece_length;	// File piece length for VINE_FILE_PIECE
	char *source;		// Name of source file, url, buffer, or literal data if an input buffer.
	char *remote_name;	// Name of file as it appears to the task.
	char *cached_name;	// Name of file in the worker's cache directory.
	char *data;		// Raw data for an input or output buffer.
};

struct vine_file * vine_file_create( const char *source, const char *remote_name, const char *data, int length, vine_file_t type, vine_file_flags_t flags );
struct vine_file *vine_file_clone( const struct vine_file *file );
void vine_file_delete( struct vine_file *f );

#endif
