/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_file.h"
#include "vine_cached_name.h"
#include "vine_task.h"

#include "copy_stream.h"
#include "debug.h"
#include "path.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <limits.h>
#include <stdlib.h>
#include <unistd.h>

/* Internal use: when the worker uses the client library, do not recompute cached names. */
int vine_hack_do_not_compute_cached_name = 0;

/* Returns file refcount. If refcount is 0, the file has been deleted. */
int vine_file_delete(struct vine_file *f)
{
	if (f) {
		f->refcount--;
		if (f->refcount > 0) {
			return f->refcount;
		}

		if (f->refcount < 0) {
			notice(D_VINE, "vine_file_delete: prevented multiple-free of file");
			return 0;
		}

		vine_task_delete(f->mini_task);
		vine_task_delete(f->recovery_task);
		free(f->source);
		free(f->cached_name);
		free(f->data);
		free(f);
	}

	return 0;
}

/* Create a new file object with the given properties. */
struct vine_file *vine_file_create(const char *source, const char *cached_name, const char *data, size_t size,
		vine_file_t type, struct vine_task *mini_task, vine_file_flags_t flags)
{
	struct vine_file *f = xxmalloc(sizeof(*f));
	memset(f, 0, sizeof(*f));

	f->source = source ? xxstrdup(source) : 0;
	f->type = type;
	f->size = size;
	f->mini_task = mini_task;
	f->recovery_task = 0;
	f->created = 0;
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
		/* If the cached name is provided, just use it.  (Likely a cloned object.) */
		f->cached_name = xxstrdup(cached_name);
	} else {
		/* Otherwise we need to figure it out ourselves from the content. */
		/* This may give us the actual size of the object along the way. */
		ssize_t totalsize = 0;
		if ((f->flags & VINE_CACHE_ALWAYS) == VINE_CACHE_ALWAYS) {
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

	return f;
}

/* Make a reference counted copy of a file object. */

struct vine_file *vine_file_clone(struct vine_file *f)
{
	if (!f)
		return 0;
	f->refcount++;
	return f;
}

/* Return the contents of the file, if available. */

const char *vine_file_contents(struct vine_file *f) { return f->data; }

/* Return the size of any kind of file. */

size_t vine_file_size(struct vine_file *f) { return f->size; }

struct vine_file *vine_file_local(const char *source, vine_file_flags_t flags)
{
	return vine_file_create(source, 0, 0, 0, VINE_FILE, 0, flags);
}

struct vine_file *vine_file_url(const char *source, vine_file_flags_t flags)
{
	return vine_file_create(source, 0, 0, 0, VINE_URL, 0, flags);
}

struct vine_file *vine_file_substitute_url(struct vine_file *f, const char *source)
{
	return vine_file_create(source, f->cached_name, 0, f->size, VINE_URL, 0, 0);
}

struct vine_file *vine_file_temp()
{
	// temp files are always cached at workers until explicitely removed.
	vine_file_flags_t flags = VINE_CACHE;

	return vine_file_create("temp", 0, 0, 0, VINE_TEMP, 0, flags);
}

struct vine_file *vine_file_buffer(const char *data, size_t size, vine_file_flags_t flags)
{
	return vine_file_create("buffer", 0, data, size, VINE_BUFFER, 0, flags);
}

struct vine_file *vine_file_empty_dir() { return vine_file_create("unnamed", 0, 0, 0, VINE_EMPTY_DIR, 0, 0); }

struct vine_file *vine_file_mini_task(struct vine_task *t, const char *name, vine_file_flags_t flags)
{
	flags |= VINE_PEER_NOSHARE; // we don't know how to share mini tasks yet.
	return vine_file_create(name, 0, 0, 0, VINE_MINI_TASK, t, flags);
}

struct vine_file *vine_file_untar(struct vine_file *f, vine_file_flags_t flags)
{
	struct vine_task *t = vine_task_create("mkdir output && tar xf input -C output");
	vine_task_add_input(t, f, "input", 0);
	vine_task_add_output(t, vine_file_local("output", flags), "output", 0);
	return vine_file_mini_task(t, "untar", flags);
}

struct vine_file *vine_file_poncho(struct vine_file *f, vine_file_flags_t flags)
{
	char *cmd = string_format("mkdir output && tar xf package.tar.gz -C output && output/bin/run_in_env");
	struct vine_task *t = vine_task_create(cmd);
	free(cmd);

	vine_task_add_input(t, f, "package.tar.gz", 0);
	vine_task_add_output(t, vine_file_local("output", flags), "output", 0);
	return vine_file_mini_task(t, "poncho", flags);
}

struct vine_file *vine_file_starch(struct vine_file *f, vine_file_flags_t flags)
{
	struct vine_task *t = vine_task_create("SFX_DIR=output SFX_EXTRACT_ONLY=1 ./package.sfx");
	vine_task_add_input(t, f, "package.sfx", 0);
	vine_task_add_output(t, vine_file_local("output", flags), "output", 0);
	return vine_file_mini_task(t, "starch", flags);
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

struct vine_file *vine_file_xrootd(
		const char *source, struct vine_file *proxy, struct vine_file *env, vine_file_flags_t flags)
{
	if (!proxy) {
		char *proxy_filename = find_x509_proxy();
		if (proxy_filename) {
			proxy = vine_file_local(proxy_filename, VINE_CACHE);
			free(proxy_filename);
		}
	}

	char *command = string_format("xrdcp %s output.root", source);
	struct vine_task *t = vine_task_create(command);

	vine_task_add_output(t, vine_file_local("output.root", flags), "output.root", 0);

	if (proxy) {
		vine_task_set_env_var(t, "X509_USER_PROXY", "proxy509");
		vine_task_add_input(t, proxy, "proxy509.pem", 0);
	}

	if (env) {
		vine_task_add_environment(t, env);
	}

	free(command);

	return vine_file_mini_task(t, "xrootd", flags);
}

struct vine_file *vine_file_chirp(const char *server, const char *source, struct vine_file *ticket,
		struct vine_file *env, vine_file_flags_t flags)
{
	char *command = string_format("chirp_get %s %s %s output.chirp",
			ticket ? "--auth=ticket --tickets=ticket.chirp" : "",
			server,
			source);

	struct vine_task *t = vine_task_create(command);

	vine_task_add_output(t, vine_file_local("output.chirp", flags), "output.chirp", 0);

	if (ticket) {
		vine_task_add_input(t, ticket, "ticket.chirp", 0);
	}

	if (env) {
		vine_task_add_environment(t, env);
	}

	free(command);

	return vine_file_mini_task(t, "chirp", flags);
}

/* vim: set noexpandtab tabstop=8: */
