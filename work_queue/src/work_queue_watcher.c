/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue_watcher.h"
#include "work_queue_process.h"
#include "work_queue_internal.h"

#include "list.h"
#include "debug.h"
#include "link.h"
#include "stringtools.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>

struct work_queue_watcher {
	struct list *watchlist;
};

struct entry {
	int64_t taskid;
	char *physical_path;
	char *logical_path;
	time_t last_checked;
	int64_t size;
};

static void entry_delete( struct entry *e )
{
	free(e->physical_path);
	free(e->physical_path);
	free(e);
}

struct work_queue_watcher * work_queue_watcher_create()
{
	struct work_queue_watcher *w = malloc(sizeof(*w));
	w->watchlist = list_create();
	return w;
}

void work_queue_watcher_delete( struct work_queue_watcher *w )
{
	struct entry *e;

	list_first_item(w->watchlist);
	while((e=list_pop_head(w->watchlist))) {
		entry_delete(e);
	}
	list_delete(w->watchlist);
	free(w);
}

void work_queue_watcher_add_process( struct work_queue_watcher *w, struct work_queue_process *p )
{
	struct work_queue_file *f;

	list_first_item(p->task->output_files);
	while((f=list_next_item(p->task->output_files))) {
		if(f->flags & WORK_QUEUE_WATCH) {
			// XXX wrap up this object

			struct entry *e = malloc(sizeof(*e));
			e->taskid = p->task->taskid;
			e->physical_path = string_format("%s/%s",p->sandbox,f->remote_name);
			e->logical_path = strdup(f->remote_name);
			e->last_checked = 0;
			e->size = 0;

			list_push_tail(w->watchlist,e);
		}
	}
}

void work_queue_watcher_remove_process( struct work_queue_watcher *w, struct work_queue_process *p )
{
	struct entry *e;
	int size = list_size(w->watchlist);
	int i;

	for(i=0;i<size;i++) {
		e = list_pop_head(w->watchlist);
		if(e->taskid == p->task->taskid) {
			entry_delete(e);
		} else {
			list_push_tail(w->watchlist,e);
		}
	}

}


int work_queue_watcher_check( struct work_queue_watcher *w )
{
	struct entry *e;

	debug(D_WQ,"checking for changed files...");

	list_first_item(w->watchlist);
	while((e=list_next_item(w->watchlist))) {
		struct stat info;
		if(stat(e->physical_path,&info)==0) {
			if(info.st_size != e->size) {
				return 1;
			}
		}
	}

	return 0;
}

int work_queue_watcher_send_changes( struct work_queue_watcher *w, struct link *master, time_t stoptime )
{
	struct entry *e;

	list_first_item(w->watchlist);
	while((e=list_next_item(w->watchlist))) {
		struct stat info;
		if(stat(e->physical_path,&info)==0) {
			if(info.st_size!=e->size) {
				int64_t offset, length;
				if(info.st_size>e->size) {
					offset = e->size;
					length = info.st_size - e->size;
					debug(D_WQ,"%s increased from %"PRId64" to %"PRId64" bytes",e->physical_path,offset,offset+length);
				} else {
					offset = 0;
					length = info.st_size;
					debug(D_WQ,"%s truncated to %"PRId64" bytes",e->physical_path,length);
				}

				int fd = open(e->physical_path,O_RDONLY);
				if(fd<0) {
					debug(D_WQ,"unable to open %s: %s",e->physical_path,strerror(errno));
					continue;
				}

				lseek(fd,offset,SEEK_SET);
				link_putfstring(master,"update %"PRId64" %s %"PRId64" %"PRId64"\n",stoptime,e->taskid,e->logical_path,offset,length);
				int actual = link_stream_from_fd(master,fd,length,stoptime);
				close(fd);
				if(actual!=length) return 0;
				e->size = info.st_size;
			}

		}
	}

	return 1;
}

