/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_file.h"

#include "debug.h"
#include "xxmalloc.h"
#include "md5.h"
#include "url_encode.h"
#include "stringtools.h"
#include "path.h"

#include <stdlib.h>
#include <unistd.h>
#include <limits.h>

/* Internal use: when the worker uses the client library, do not recompute cached names. */
int ds_hack_do_not_compute_cached_name = 0;

/*
For a given task and file, generate the name under which the file
should be stored in the remote cache directory.

The basic strategy is to construct a name that is unique to the
namespace from where the file is drawn, so that tasks sharing
the same input file can share the same copy.

In the common case of files, the cached name is based on the
hash of the local path, with the basename of the local path
included simply to assist with debugging.

In each of the other file types, a similar approach is taken,
including a hash and a name where one is known, or another
unique identifier where no name is available.
*/

char *make_cached_name( const struct ds_file *f )
{
	static unsigned int file_count = 0;
	file_count++;

	/* Default of source is remote name (needed only for directories) */
	char *source = f->source ? f->source : f->remote_name;

	unsigned char digest[MD5_DIGEST_LENGTH];
	char source_enc[PATH_MAX];

	if(f->type == DS_BUFFER) {
		//dummy digest for buffers
		md5_buffer("buffer", 6, digest);
	} else {
		md5_buffer(source,strlen(source),digest);
		url_encode(path_basename(source), source_enc, PATH_MAX);
	}

	/* 0 for cache files, file_count for non-cache files. With this, non-cache
	 * files cannot be shared among tasks, and can be safely deleted once a
	 * task finishes. */
	unsigned int cache_file_id = 0;
	if(!(f->flags & DS_CACHE)) {
		cache_file_id = file_count;
	}

	switch(f->type) {
		case DS_FILE:
		case DS_EMPTY_DIR:
			return string_format("file-%d-%s-%s", cache_file_id, md5_string(digest), source_enc);
			break;
		case DS_FILE_PIECE:
			return string_format("piece-%d-%s-%s-%lld-%lld",cache_file_id, md5_string(digest),source_enc,(long long)f->offset,(long long)f->piece_length);
			break;
		case DS_COMMAND:
			return string_format("cmd-%d-%s", cache_file_id, md5_string(digest));
			break;
		case DS_URL:
			return string_format("url-%d-%s", cache_file_id, md5_string(digest));
			break;
		case DS_BUFFER:
		default:
			return string_format("buffer-%d-%s", cache_file_id, md5_string(digest));
			break;
	}
}

/* Create a new file object with the given properties. */

struct ds_file *ds_file_create(const char *source, const char *remote_name, ds_file_t type, ds_file_flags_t flags)
{
	struct ds_file *f;

	f = malloc(sizeof(*f));
	if(!f) {
		debug(D_NOTICE, "Cannot allocate memory for file %s.\n", remote_name);
		return NULL;
	}

	memset(f, 0, sizeof(*f));

	f->remote_name = xxstrdup(remote_name);
	f->type = type;
	f->flags = flags;

	/* DS_BUFFER needs to set these after the current function returns */
	if(source) {
		f->source = xxstrdup(source);
		f->length  = strlen(source);
	}

	if(ds_hack_do_not_compute_cached_name) {
  		f->cached_name = xxstrdup(f->source);
	} else {
		f->cached_name = make_cached_name(f);
	}
	
	return f;
}

/* Make a deep copy of a file object to be used independently. */

struct ds_file *ds_file_clone(const struct ds_file *file)
{
	return ds_file_create(file->source,file->remote_name,file->type,file->flags);
}

/* Delete a file object */

void ds_file_delete(struct ds_file *tf)
{
	if(tf->source)
		free(tf->source);
	if(tf->remote_name)
		free(tf->remote_name);
	if(tf->cached_name)
		free(tf->cached_name);
	free(tf);
}
