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

typedef enum {
  VINE_FILE_STATE_PENDING, /**< This file has not yet been created by a task. */
  VINE_FILE_STATE_CREATED   /**< This file has been created at some point.  (although it might have been lost!) */
} vine_file_state_t;

struct vine_file {
	vine_file_type_t  type;  // Type of data source: VINE_FILE, VINE_BUFFER, VINE_URL, etc.
	vine_file_flags_t flags; // Whether or not to transfer this file between workers.
	vine_file_state_t state; // Whether the file is PENDING or has been CREATED
	vine_cache_level_t cache_level; // How aggressively this file should be cached.
	char *source;       // Name of source file, url, buffer.
	char *cached_name;  // Name of file in the worker's cache directory.
	size_t size;        // Length of source data, if known.
	time_t mtime;       // Modification time of source data, if known.
	mode_t mode;        // Manual override for Unix mode bits sent to worker.  Zero if unset.
	char *data;         // Raw data for an input or output buffer.
	struct vine_task *mini_task; // Mini task used to generate the desired output file.
	struct vine_task *recovery_task; // For temp files, a copy of the task that created it.
	int original_producer_task_id;   // For temp files, the task ID of the original task that produces this file.
	struct vine_worker_info *source_worker; // if this is a substitute file, attach the worker serving it. 
	int change_message_shown; // True if error message already shown.
	int refcount;       // Number of references from a task object, delete when zero.
};

struct vine_file * vine_file_create( const char *source, const char *cached_name, const char *data, size_t size, vine_file_type_t type, struct vine_task *mini_task, vine_cache_level_t cache_level, vine_file_flags_t flags);

struct vine_file * vine_file_substitute_url( struct vine_file *f, const char *source, struct vine_worker_info *w );

struct vine_file *vine_file_addref( struct vine_file *f );

/* Decreases reference count of file, and frees if zero. */
int vine_file_delete( struct vine_file *f );

int vine_file_has_changed( struct vine_file *f );

char * vine_file_make_file_url( const char * source);

struct vine_file *vine_file_local( const char *source, vine_cache_level_t cache, vine_file_flags_t flags );
struct vine_file *vine_file_url( const char *source, vine_cache_level_t cache, vine_file_flags_t flags );
struct vine_file *vine_file_temp();
struct vine_file *vine_file_temp_no_peers();
struct vine_file *vine_file_buffer( const char *buffer, size_t size, vine_cache_level_t cache, vine_file_flags_t flags );
struct vine_file *vine_file_mini_task( struct vine_task *t, const char *name, vine_cache_level_t cache, vine_file_flags_t flags );
struct vine_file *vine_file_untar( struct vine_file *f, vine_cache_level_t cache, vine_file_flags_t flags );
struct vine_file *vine_file_poncho( struct vine_file *f, vine_cache_level_t cache, vine_file_flags_t flags );
struct vine_file *vine_file_starch( struct vine_file *f, vine_cache_level_t cache, vine_file_flags_t flags );
struct vine_file *vine_file_xrootd( const char *source, struct vine_file *proxy, struct vine_file *env, vine_cache_level_t cache, vine_file_flags_t flags );
struct vine_file *vine_file_chirp( const char *server, const char *source, struct vine_file *ticket, struct vine_file *env, vine_cache_level_t cache, vine_file_flags_t flags );

#endif
