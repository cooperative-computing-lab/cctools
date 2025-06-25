/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_file.h"
#include "vine_cached_name.h"
#include "vine_counters.h"
#include "vine_task.h"

#include "copy_stream.h"
#include "debug.h"
#include "path.h"
#include "stringtools.h"
#include "timestamp.h"
#include "unlink_recursive.h"
#include "uuid.h"
#include "xxmalloc.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

/* Internal use: when the worker uses the client library, do not recompute cached names. */
int vine_hack_do_not_compute_cached_name = 0;

/* Returns file refcount. If refcount is 0, the file has been deleted. */
int vine_file_delete(struct vine_file *f)
{
	if (f) {
		f->refcount--;

		vine_counters.file.deleted++;

		if (f->refcount == 1 && f->recovery_task) {
			/* delete the recovery task for this file, if any, to break the refcount cycle.
			 * (The f and f->recovery_task have pointers to each other.) */
			struct vine_task *rt = f->recovery_task;
			f->recovery_task = NULL;
			vine_task_delete(rt);
			return 0;
		}

		if (f->refcount > 0) {
			return f->refcount;
		}

		if (f->refcount < 0) {
			debug(D_VINE, "vine_file_delete: prevented multiple-free of file: %s", f->source);
			return 0;
		}

		if (f->type == VINE_FILE && f->flags & VINE_UNLINK_WHEN_DONE) {
			/* when an VINE_UNLINK_WHEN_DONE file is requested to be deleted, and its refcount is 1, this
			 * means that no task is using it. The only reference is the one made when created. Thus, we
			 * delete it here.*/
			timestamp_t start_time = timestamp_get();
			unlink_recursive(f->source);
			timestamp_t end_time = timestamp_get();

			debug(D_VINE, "vine_file_delete: deleting %s on reference count took: %.03lfs", f->source, (end_time - start_time) / 1000000.00);
		}

		vine_task_delete(f->mini_task);
		free(f->source);
		free(f->cached_name);
		free(f->data);
		free(f);
	}

	return 0;
}

/* Create a new file object with the given properties. */
struct vine_file *vine_file_create(const char *source, const char *cached_name, const char *data, size_t size, vine_file_type_t type, struct vine_task *mini_task,
		vine_cache_level_t cache_level, vine_file_flags_t flags)
{
	struct vine_file *f = xxmalloc(sizeof(*f));
	memset(f, 0, sizeof(*f));

	f->source = source ? xxstrdup(source) : 0;
	f->source_worker = 0;
	f->type = type;
	f->size = size;
	f->mode = 0;
	f->mini_task = mini_task;
	f->recovery_task = 0;
	f->original_producer_task_id = 0;
	f->state = VINE_FILE_STATE_CREATED; /* Assume state created until told otherwise */
	f->cache_level = cache_level;
	f->flags = flags;

	if (data) {
		/* Terminate with a null, just in case the user tries to treat this as a C string. */
		f->data = malloc(size + 1);
		memcpy(f->data, data, size);
		f->data[size] = 0;
	} else {
		f->data = 0;
	}

	if (vine_hack_do_not_compute_cached_name) {
		/* On the worker, the source (name on disk) is already the cached name. */
		f->cached_name = xxstrdup(f->source);
	} else if (cached_name) {
		/* If the cached name is provided, just use it.  (Likely a referenced object.) */
		f->cached_name = xxstrdup(cached_name);
	} else {
		/* Otherwise we need to figure it out ourselves from the content. */
		/* This may give us the actual size of the object along the way. */
		ssize_t totalsize = 0;
		if (f->cache_level >= VINE_CACHE_LEVEL_WORKER) {
			f->cached_name = vine_cached_name(f, &totalsize);
		} else {
			if (f->type == VINE_FILE) {
				f->cached_name = vine_meta_name(f, &totalsize);
				/* if this is a pending file give it a random name */
				if (!f->cached_name) {
					f->cached_name = vine_random_name(f, &totalsize);
				}
			} else {
				f->cached_name = vine_random_name(f, &totalsize);
			}
		}
		if (size == 0) {
			f->size = totalsize;
		}
	}

	f->refcount = 1;
	vine_counters.file.created++;

	return f;
}

/* Make a reference counted copy of a file object. */

struct vine_file *vine_file_addref(struct vine_file *f)
{
	if (!f)
		return 0;
	f->refcount++;
	vine_counters.file.ref_added++;
	return f;
}

/* Make a URL reference to a file source*/

char *vine_file_make_file_url(const char *source)
{

	char *abs_path = path_getcwd();

	char *result = string_format("file://%s/%s", abs_path, source);

	free(abs_path);

	return result;
}

/* Return the contents of the file, if available. */

const char *vine_file_contents(struct vine_file *f)
{
	if (f) {
		return f->data;
	}

	return NULL;
}

/* Return the size of any kind of file. */

size_t vine_file_size(struct vine_file *f)
{
	if (f) {
		return f->size;
	}

	return 0;
}

/*
Return true if the source of this file has changed since it was first used.
This should not happen, it indicates a violation of the workflow semantics.
*/

int vine_file_has_changed(struct vine_file *f)
{
	if (f->type == VINE_FILE) {

		struct stat info;

		int result = lstat(f->source, &info);
		if (result != 0) {
			debug(D_NOTICE | D_VINE, "input file %s couldn't be accessed: %s", f->source, strerror(errno));
			return 1;
		}

		if (f->mtime == 0) {
			/* If we have not observed time and size before, capture it now. */
			f->mtime = info.st_mtime;
			f->size = info.st_size;
		} else {
			/* If we have seen it before, it should not have changed. */
			if (f->mtime != info.st_mtime || (!S_ISDIR(info.st_mode) && ((int64_t)f->size) != ((int64_t)info.st_size))) {
				if (!f->change_message_shown) {
					debug(D_VINE | D_NOTICE,
							"input file %s was modified by someone in the middle of the workflow! Workers may use different versions of the file.\n",
							f->source);
					f->change_message_shown++;
				}
				// XXX: do nothing for now, as some workflows break after
				// updating the file times without modifying its contents.
				return 0;
			}
		}
	}

	return 0;
}

struct vine_file *vine_file_local(const char *source, vine_cache_level_t cache, vine_file_flags_t flags)
{
	return vine_file_create(source, 0, 0, 0, VINE_FILE, 0, cache, flags);
}

struct vine_file *vine_file_url(const char *source, vine_cache_level_t cache, vine_file_flags_t flags)
{
	return vine_file_create(source, 0, 0, 0, VINE_URL, 0, cache, flags);
}

struct vine_file *vine_file_substitute_url(struct vine_file *f, const char *source, struct vine_worker_info *w)
{
	struct vine_file *sub = vine_file_create(source, f->cached_name, 0, f->size, VINE_URL, 0, 0, 0);
	sub->source_worker = w;
	return sub;
}

struct vine_file *vine_file_temp()
{
	// temp files are always cached at workers until explicitely removed.
	vine_cache_level_t cache = VINE_CACHE_LEVEL_WORKFLOW;

	return vine_file_create("temp", 0, 0, 0, VINE_TEMP, 0, cache, 0);
}

struct vine_file *vine_file_temp_no_peers()
{
	// temp files are always cached at workers until explicitely removed.
	vine_cache_level_t cache = VINE_CACHE_LEVEL_WORKFLOW;
	cctools_uuid_t uuid;
	cctools_uuid_create(&uuid);

	char *name = string_format("temp-local-%s", uuid.str);
	return vine_file_create(name, 0, 0, 0, VINE_FILE, 0, cache, VINE_UNLINK_WHEN_DONE);
	free(name);
}

struct vine_file *vine_file_buffer(const char *data, size_t size, vine_cache_level_t cache, vine_file_flags_t flags)
{
	return vine_file_create("buffer", 0, data, size, VINE_BUFFER, 0, cache, flags);
}

struct vine_file *vine_file_mini_task(struct vine_task *t, const char *name, vine_cache_level_t cache, vine_file_flags_t flags)
{
	flags |= VINE_PEER_NOSHARE; // we don't know how to share mini tasks yet.
	return vine_file_create(name, 0, 0, 0, VINE_MINI_TASK, t, cache, flags);
}

struct vine_file *vine_file_untar(struct vine_file *f, vine_cache_level_t cache, vine_file_flags_t flags)
{
	struct vine_task *t = vine_task_create("mkdir output && tar xf input -C output");
	vine_task_add_input(t, f, "input", 0);
	return vine_file_mini_task(t, "output", cache, flags);
}

struct vine_file *vine_file_poncho(struct vine_file *f, vine_cache_level_t cache, vine_file_flags_t flags)
{
	char *cmd = string_format("mkdir output && tar xf package.tar.gz -C output && output/bin/run_in_env");
	struct vine_task *t = vine_task_create(cmd);
	free(cmd);

	vine_task_add_input(t, f, "package.tar.gz", 0);
	return vine_file_mini_task(t, "output", cache, flags);
}

struct vine_file *vine_file_starch(struct vine_file *f, vine_cache_level_t cache, vine_file_flags_t flags)
{
	struct vine_task *t = vine_task_create("SFX_DIR=output SFX_EXTRACT_ONLY=1 ./package.sfx");
	vine_task_add_input(t, f, "package.sfx", 0);
	return vine_file_mini_task(t, "output", cache, flags);
}

static char *find_x509_proxy()
{
	const char *from_env = getenv("X509_USER_PROXY");

	if (from_env) {
		return xxstrdup(from_env);
	} else {
		uid_t uid = getuid();
		const char *tmpdir = getenv("TMPDIR");
		if (!tmpdir) {
			tmpdir = "/tmp";
		}

		char *from_uid = string_format("%s/x509up_u%u", tmpdir, uid);
		if (!access(from_uid, R_OK)) {
			return from_uid;
		}
	}

	return NULL;
}

struct vine_file *vine_file_xrootd(const char *source, struct vine_file *proxy, struct vine_file *env, vine_cache_level_t cache, vine_file_flags_t flags)
{
	if (!proxy) {
		char *proxy_filename = find_x509_proxy();
		if (proxy_filename) {
			proxy = vine_file_local(proxy_filename, VINE_CACHE_LEVEL_WORKFLOW, 0);
			free(proxy_filename);
		}
	}

	char *command = string_format("xrdcp %s output.root", source);
	struct vine_task *t = vine_task_create(command);

	if (proxy) {
		vine_task_set_env_var(t, "X509_USER_PROXY", "proxy509");
		vine_task_add_input(t, proxy, "proxy509.pem", 0);
	}

	if (env) {
		vine_task_add_environment(t, env);
	}

	free(command);

	return vine_file_mini_task(t, "output.root", cache, flags);
}

struct vine_file *vine_file_chirp(const char *server, const char *source, struct vine_file *ticket, struct vine_file *env, vine_cache_level_t cache, vine_file_flags_t flags)
{
	char *command = string_format("chirp_get %s %s %s output.chirp", ticket ? "--auth=ticket --tickets=ticket.chirp" : "", server, source);

	struct vine_task *t = vine_task_create(command);

	if (ticket) {
		vine_task_add_input(t, ticket, "ticket.chirp", 0);
	}

	if (env) {
		vine_task_add_environment(t, env);
	}

	free(command);

	return vine_file_mini_task(t, "output.chirp", cache, flags);
}

vine_file_type_t vine_file_type(struct vine_file *f)
{
	return f->type;
}

const char *vine_file_source(struct vine_file *f)
{
	return f->source;
}

void vine_file_set_mode(struct vine_file *f, int mode)
{
	/* The mode must contain, at a minimum, owner-rw (0600) (so that we can delete it) */
	/* And it should not contain anything beyond the standard 0777. */
	f->mode = (mode | 0600) & 0777;
}

/* vim: set noexpandtab tabstop=8: */
