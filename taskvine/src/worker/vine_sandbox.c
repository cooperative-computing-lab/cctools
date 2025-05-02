/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_sandbox.h"
#include "vine_cache.h"
#include "vine_cache_file.h"
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
Determine whether all the files needed by this process are present.
Return VINE_STATUS_READY if all are present.
Return VINE_STATUS_PROCESSING if some are not ready.
Return VINE_STATUS_FAILED if some have definitely failed.
*/

vine_cache_status_t vine_sandbox_ensure(struct vine_process *p, struct vine_cache *cache, struct link *manager, struct itable *procs_table)
{
	int processing = 0;

	struct vine_mount *m;
	LIST_ITERATE(p->task->input_mounts, m)
	{
		vine_cache_status_t cache_status = vine_cache_ensure(cache, m->file->cached_name);

		switch (cache_status) {
		case VINE_CACHE_STATUS_PENDING:
		case VINE_CACHE_STATUS_PROCESSING:
		case VINE_CACHE_STATUS_TRANSFERRED:
			processing++;
			break;
		case VINE_CACHE_STATUS_READY:
			break;
		case VINE_CACHE_STATUS_UNKNOWN: {
			struct vine_process *lp;
			uint64_t task_id;
			int found_file = 0;
			ITABLE_ITERATE(procs_table, task_id, lp)
			{
				struct vine_mount *lm;
				LIST_ITERATE(lp->task->output_mounts, lm)
				{
					if (strcmp(lm->file->cached_name, m->file->cached_name) == 0) {
						found_file = 1;
						break;
					}
				}
				if (found_file) {
					break;
				}
			}
			if (found_file) {
				processing++;
				break;
			}
		}
			return VINE_CACHE_STATUS_FAILED;
		case VINE_CACHE_STATUS_FAILED:
			return VINE_CACHE_STATUS_FAILED;
		}
	}

	if (processing) {
		return VINE_CACHE_STATUS_PROCESSING;
	} else {
		return VINE_CACHE_STATUS_READY;
	}
}

/*
Ensure that a given input file/dir/object is present in the cache,
(which should have occurred from a prior transfer)
and then link it into the sandbox at the desired location.
*/

static int stage_input_file(struct vine_process *p, struct vine_mount *m, struct vine_file *f, struct vine_cache *cache)
{
	char *cache_path = vine_cache_data_path(cache, f->cached_name);
	char *sandbox_path = vine_sandbox_full_path(p, m->remote_name);

	int result = 0;

	vine_cache_status_t status;
	status = vine_cache_ensure(cache, f->cached_name);
	if (status == VINE_CACHE_STATUS_READY) {
		create_dir_parents(sandbox_path, 0777);
		debug(D_VINE, "input: link %s -> %s", cache_path, sandbox_path);
		if (m->flags & VINE_MOUNT_SYMLINK) {
			/* If the user has requested a symlink, just do that b/c it is faster for large dirs. */
			result = symlink(cache_path, sandbox_path);
			/* Change sense of Unix result to true/false. */
			result = !result;
		} else {
			/* Otherwise recursively hard-link the object into the sandbox. */
			result = file_link_recursive(cache_path, sandbox_path, 1);
		}

		if (!result)
			debug(D_VINE, "couldn't link %s into sandbox as %s: %s", cache_path, sandbox_path, strerror(errno));
	} else {
		debug(D_VINE, "input: %s is not ready in the cache!", f->cached_name);
		result = 0;
	}

	free(cache_path);
	free(sandbox_path);

	return result;
}

/* Create an empty output directory when requested by VINE_MOUNT_MKDIR */

static int create_empty_output_dir(struct vine_process *p, struct vine_mount *m)
{
	char *sandbox_path = vine_sandbox_full_path(p, m->remote_name);

	int result = mkdir(sandbox_path, 0755);
	if (result != 0) {
		debug(D_VINE, "sandbox: couldn't mkdir %s: %s", sandbox_path, strerror(errno));
		free(sandbox_path);
		return 0;
	} else {
		free(sandbox_path);
		return 1;
	}
}

/*
For each input file specified by the process,
stage it into the sandbox directory from the cache.
*/

int vine_sandbox_stagein(struct vine_process *p, struct vine_cache *cache)
{
	struct vine_task *t = p->task;
	int result = 1;

	struct vine_mount *m;

	/* For each input mount, stage it into the sandbox. */

	LIST_ITERATE(t->input_mounts, m)
	{
		result = stage_input_file(p, m, m->file, cache);
		if (!result)
			break;
	}

	/* If any of the output mounts have the MKDIR flag, then create those empty dirs. */

	LIST_ITERATE(t->output_mounts, m)
	{
		if (m->flags & VINE_MOUNT_MKDIR) {
			result = create_empty_output_dir(p, m);
			if (!result)
				break;
		}
	}

	return result;
}

/*
Move a given output file back to the target cache location
and inform the manager of the added file.
*/

static int stage_output_file(struct vine_process *p, struct vine_mount *m, struct vine_file *f, struct vine_cache *cache, struct link *manager)
{
	char *cache_path = vine_cache_data_path(cache, f->cached_name);
	char *sandbox_path = vine_sandbox_full_path(p, m->remote_name);

	int result = 0;
	int mode;
	time_t mtime;
	int64_t size;

	timestamp_t transfer_time = p->execution_end - p->execution_start;

	debug(D_VINE, "output: measuring %s", sandbox_path);
	if (vine_cache_file_measure_metadata(sandbox_path, &mode, &size, &mtime)) {
		debug(D_VINE, "output: moving %s to %s", sandbox_path, cache_path);
		if (vine_cache_add_file(cache, f->cached_name, sandbox_path, f->cache_level, mode, size, mtime, p->execution_start, transfer_time, manager)) {
			f->size = size;
			result = 1;
		} else {
			debug(D_VINE, "output: unable to move %s to %s: %s\n", sandbox_path, cache_path, strerror(errno));
			result = 0;
		}
	} else {
		debug(D_VINE, "output: unable to measure size of %s: %s\n", sandbox_path, strerror(errno));
		result = 0;
	}

	free(cache_path);
	free(sandbox_path);

	return result;
}

/*
Move all output files of a completed process back into the proper cache location.
This function deliberately does not fail.  If any of the desired outputs was not
created, we still want the task to be marked as completed and sent back to the
manager.

The manager will handle the consequences of missing output files when processing
the result of the task. Therefore, this function should be call BEFORE the task is
reported as done to the manager.
*/

void vine_sandbox_stageout(struct vine_process *p, struct vine_cache *cache, struct link *manager)
{
	struct vine_mount *m;
	LIST_ITERATE(p->task->output_mounts, m)
	{
		stage_output_file(p, m, m->file, cache, manager);
	}
}
