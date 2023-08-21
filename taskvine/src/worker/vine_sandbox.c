/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_sandbox.h"
#include "vine_cache.h"
#include "vine_file.h"
#include "vine_mount.h"
#include "vine_task.h"
#include "vine_worker.h"

#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "file_link_recursive.h"
#include "stringtools.h"

#include <errno.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

char *vine_sandbox_full_path(struct vine_process *p, const char *sandbox_name)
{
	return string_format("%s/%s", p->sandbox, sandbox_name);
}

/*
Ensures that each input file is present.
*/

vine_cache_status_t vine_sandbox_ensure(struct vine_process *p, struct vine_cache *cache, struct link *manager)
{
	int processing = 0;
	struct vine_task *t = p->task;
	vine_cache_status_t cache_status = VINE_CACHE_STATUS_READY;

	if (t->input_mounts) {
		struct vine_mount *m;
		LIST_ITERATE(t->input_mounts, m)
		{
			cache_status = vine_cache_ensure(cache, m->file->cached_name);
			if (cache_status == VINE_CACHE_STATUS_PROCESSING)
				processing = 1;
			if (cache_status == VINE_CACHE_STATUS_FAILED)
				break;
		}
	}
	if (cache_status == VINE_CACHE_STATUS_FAILED)
		return VINE_CACHE_STATUS_FAILED;
	if (processing)
		return VINE_CACHE_STATUS_PROCESSING;
	return VINE_CACHE_STATUS_READY;
}

/*
Ensure that a given input file/dir/object is present in the cache,
(which should have occurred from a prior transfer)
and then link it into the sandbox at the desired location.
*/

static int stage_input_file(struct vine_process *p, struct vine_mount *m, struct vine_file *f, struct vine_cache *cache)
{
	char *cache_path = vine_cache_full_path(cache, f->cached_name);
	char *sandbox_path = vine_sandbox_full_path(p, m->remote_name);

	int result = 0;

	if (f->type == VINE_EMPTY_DIR) {
		/* Special case: empty directories are not cached objects, just create in sandbox */
		result = create_dir(sandbox_path, 0700);
		if (!result)
			debug(D_VINE, "couldn't create directory %s: %s", sandbox_path, strerror(errno));

	} else {
		/* All other types, link the cached object into the sandbox */
		vine_cache_status_t status;
		status = vine_cache_ensure(cache, f->cached_name);
		if (status == VINE_CACHE_STATUS_READY) {
			create_dir_parents(sandbox_path, 0777);
			debug(D_VINE, "input: link %s -> %s", cache_path, sandbox_path);
			result = file_link_recursive(cache_path, sandbox_path, vine_worker_symlinks_enabled);
			if (!result)
				debug(D_VINE,
						"couldn't link %s into sandbox as %s: %s",
						cache_path,
						sandbox_path,
						strerror(errno));
		} else {
			debug(D_VINE, "input: %s is not ready in the cache!", f->cached_name);
			result = 0;
		}
	}

	free(cache_path);
	free(sandbox_path);

	return result;
}

/*
For each input file specified by the process,
stage it into the sandbox directory from the cache.
*/

int vine_sandbox_stagein(struct vine_process *p, struct vine_cache *cache)
{
	struct vine_task *t = p->task;
	int result = 1;

	if (t->input_mounts) {
		struct vine_mount *m;
		LIST_ITERATE(t->input_mounts, m)
		{
			result = stage_input_file(p, m, m->file, cache);
			if (!result)
				break;
		}
	}

	return result;
}

/*
Move a given output file back to the target cache location.
First attempt a cheap rename.
If that does not work (perhaps due to crossing filesystems)
then attempt a recursive copy.
Inform the cache of the added file.
*/

static int stage_output_file(struct vine_process *p, struct vine_mount *m, struct vine_file *f,
		struct vine_cache *cache, struct link *manager)
{
	char *cache_path = vine_cache_full_path(cache, f->cached_name);
	char *sandbox_path = vine_sandbox_full_path(p, m->remote_name);

	int result = 0;

	debug(D_VINE, "output: moving %s to %s", sandbox_path, cache_path);
	if (rename(sandbox_path, cache_path) < 0) {
		debug(D_VINE,
				"output: move failed, attempting copy of %s to %s: %s",
				sandbox_path,
				cache_path,
				strerror(errno));
		if (copy_file_to_file(sandbox_path, cache_path) == -1) {
			debug(D_VINE,
					"could not move or copy output file %s to %s: %s",
					sandbox_path,
					cache_path,
					strerror(errno));
			result = 0;
		} else {
			result = 1;
		}
	} else {
		result = 1;
	}

	if (result) {
		struct stat info;
		if (stat(cache_path, &info) == 0) {
			vine_cache_addfile(cache, info.st_size, info.st_mode, f->cached_name);
			vine_worker_send_cache_update(manager, f->cached_name, info.st_size, 0, 0);
		} else {
			// This seems implausible given that the rename/copy succeded, but we still have to check...
			debug(D_VINE, "output: failed to stat %s: %s", cache_path, strerror(errno));
			result = 0;
		}
	}

	free(sandbox_path);
	free(cache_path);

	return result;
}

/*
Move all output files of a completed process back into the proper cache location.
This function deliberately does not fail.  If any of the desired outputs was not
created, we still want the task to be marked as completed and sent back to the
manager.  The manager will handle the consequences of missing output files.
*/

int vine_sandbox_stageout(struct vine_process *p, struct vine_cache *cache, struct link *manager)
{
	struct vine_mount *m;
	LIST_ITERATE(p->task->output_mounts, m) { stage_output_file(p, m, m->file, cache, manager); }

	return 1;
}
