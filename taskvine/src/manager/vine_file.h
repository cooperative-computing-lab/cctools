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
mapped into a tasks' working directory via a vine_mount.
As a result, it has several kinds of names:

- f->source indicates the name of the source file, url, or command that provides the data.
- f->cached_name indicates the name of the file as it is stored in the worker's cache.

This module is private to the manager and should not be invoked by the end user.
*/

#include "taskvine.h"

#include <sys/types.h>

/** Select the type of an input or output file to attach to a task. */

typedef enum {
	VINE_FILE = 1,              /**< A file or directory present at the manager. **/
	VINE_URL,                   /**< A file obtained by downloading from a URL. */
	VINE_TEMP,		    /**< A temporary file created as an output of a task. */
	VINE_BUFFER,                /**< A file obtained from data in the manager's memory space. */
	VINE_MINI_TASK,             /**< A file obtained by executing a Unix command line. */
	VINE_EMPTY_DIR,              /**< An empty directory to create in the task sandbox. */
} vine_file_t;

struct vine_file {
	vine_file_t type;   // Type of data source: VINE_FILE, VINE_BUFFER, VINE_URL, etc.
	char *source;       // Name of source file, url, buffer.
	char *cached_name;  // Name of file in the worker's cache directory.
	size_t size;        // Length of source data, if known.
	char *data;         // Raw data for an input or output buffer.
	struct vine_task *mini_task; // Mini task used to generate the desired output file.
	int refcount;       // Number of references from a task object, delete when zero.
	vine_file_flags_t flags; // whether or not to transfer this file between workers.
};

struct vine_file * vine_file_create( const char *source, const char *cached_name, const char *data, size_t size, vine_file_t type, struct vine_task *mini_task, vine_file_flags_t flags);

struct vine_file * vine_file_substitute_url( struct vine_file *f, const char *source );

struct vine_file *vine_file_clone( struct vine_file *f );

/* Decreases reference count of file, and frees if zero. */
int vine_file_delete( struct vine_file *f );

struct vine_file *vine_file_local( const char *source, vine_file_flags_t flags );
struct vine_file *vine_file_url( const char *source, vine_file_flags_t flags );
struct vine_file *vine_file_temp();
struct vine_file *vine_file_buffer( const char *buffer, size_t size, vine_file_flags_t flags );
struct vine_file *vine_file_empty_dir( );
struct vine_file *vine_file_mini_task( struct vine_task *t, vine_file_flags_t flags );
struct vine_file *vine_file_untar( struct vine_file *f, vine_file_flags_t flags );
struct vine_file *vine_file_poncho( struct vine_file *f, vine_file_flags_t flags );
struct vine_file *vine_file_starch( struct vine_file *f, vine_file_flags_t flags );
struct vine_file *vine_file_xrootd( const char *source, struct vine_file *proxy, struct vine_file *env, vine_file_flags_t flags );
struct vine_file *vine_file_chirp( const char *server, const char *source, struct vine_file *ticket, struct vine_file *env, vine_file_flags_t flags );

#endif
