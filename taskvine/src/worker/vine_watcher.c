/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_watcher.h"
#include "vine_file.h"
#include "vine_mount.h"
#include "vine_process.h"

#include "debug.h"
#include "link.h"
#include "list.h"
#include "stringtools.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/*
The watcher keeps a linked list of files that must be watched.
For each one, it tracks the path and size (obviously) but also
the task_id and logical path, so that it can send back enough
info for the manager to match the updates up with the right file.
*/

struct vine_watcher {
	struct list *watchlist;
};

struct entry {
	int64_t task_id;
	char *physical_path;
	char *logical_path;
	int64_t size;
	int do_not_watch;
};

static void entry_delete(struct entry *e)
{
	free(e->physical_path);
	free(e->logical_path);
	free(e);
}

static struct entry *entry_create(int64_t task_id, char *physical_path, char *logical_path)
{
	struct entry *e = malloc(sizeof(*e));
	e->task_id = task_id;
	e->physical_path = physical_path;
	e->logical_path = logical_path;
	e->size = 0;
	e->do_not_watch = 0;
	return e;
}

struct vine_watcher *vine_watcher_create()
{
	struct vine_watcher *w = malloc(sizeof(*w));
	w->watchlist = list_create();
	return w;
}

void vine_watcher_delete(struct vine_watcher *w)
{
	struct entry *e;

	list_first_item(w->watchlist);
	while ((e = list_pop_head(w->watchlist))) {
		entry_delete(e);
	}
	list_delete(w->watchlist);
	free(w);
}

/*
For each watched file in this process, add an entry to the watcher list.
If the process has no watched files, then nothing is kept.
Note that the path of the watched file is relative to the sandbox
directory chosen for the running process.
*/

void vine_watcher_add_process(struct vine_watcher *w, struct vine_process *p)
{
	struct vine_mount *m;

	LIST_ITERATE(p->task->output_mounts, m)
	{

		if (m->flags & VINE_WATCH) {

			struct entry *e;
			e = entry_create(p->task->task_id,
					string_format("%s/%s", p->sandbox, m->remote_name),
					strdup(m->remote_name));

			list_push_tail(w->watchlist, e);
		}
	}
}

/*
Remove any watched files associated with the given process.
*/

void vine_watcher_remove_process(struct vine_watcher *w, struct vine_process *p)
{
	struct entry *e;
	int size = list_size(w->watchlist);
	int i;

	for (i = 0; i < size; i++) {
		e = list_pop_head(w->watchlist);
		if (e->task_id == p->task->task_id) {
			entry_delete(e);
		} else {
			list_push_tail(w->watchlist, e);
		}
	}
}

/*
Check to see if any watched files have changed since the last look.
If any one file has changed, it is not necessary to look for any more,
since the files will be rescanned in vine_watcher_send_results.
Also, note that the debug message does not print the specific file;
we don't want the user to be thrown off by missing messages about
files not examined.
*/

int vine_watcher_check(struct vine_watcher *w)
{
	struct entry *e;

	LIST_ITERATE(w->watchlist, e)
	{
		struct stat info;
		if (e->do_not_watch)
			continue;
		if (stat(e->physical_path, &info) == 0) {
			if (info.st_size != e->size) {
				debug(D_VINE, "watched files have changed");
				return 1;
			}
		}
	}

	return 0;
}

/*
Scan over all watched files, and send back any changes since the last check.
This feature is designed to work with files that are accessed append-only.
If the file has shrunk since the last measurement, then we mark the file
as non-append and stop watching it.
If the file is not accessible or there is some other problem,
don't take any drastic action, because it does not (necessarily)
indicate a task failure.
In all cases, the complete file is sent back in the normal way
when the task ends, to ensure reliable output.
*/

int vine_watcher_send_changes(struct vine_watcher *w, struct link *manager, time_t stoptime)
{
	struct entry *e;

	LIST_ITERATE(w->watchlist, e)
	{
		struct stat info;
		if (e->do_not_watch)
			continue;
		if (stat(e->physical_path, &info) == 0) {
			if (info.st_size > e->size) {
				int64_t offset = e->size;
				int64_t length = info.st_size - e->size;
				debug(D_VINE,
						"%s increased from %" PRId64 " to %" PRId64 " bytes",
						e->physical_path,
						offset,
						offset + length);
				int fd = open(e->physical_path, O_RDONLY);
				if (fd < 0) {
					debug(D_VINE, "unable to open %s: %s", e->physical_path, strerror(errno));
					continue;
				}

				lseek(fd, offset, SEEK_SET);
				link_printf(manager,
						stoptime,
						"update %" PRId64 " %s %" PRId64 " %" PRId64 "\n",
						e->task_id,
						e->logical_path,
						offset,
						length);
				int actual = link_stream_from_fd(manager, fd, length, stoptime);
				close(fd);
				if (actual != length)
					return 0;
				e->size = info.st_size;
			} else if (info.st_size < e->size) {
				debug(D_VINE,
						"%s unexpectedly shrank from %" PRId64 " to %" PRId64 " bytes",
						e->physical_path,
						(int64_t)e->size,
						(int64_t)info.st_size);
				debug(D_VINE, "%s will no longer be watched for changes", e->physical_path);
				e->do_not_watch = 1;
			}
		}
	}

	return 1;
}

/* vim: set noexpandtab tabstop=4: */
