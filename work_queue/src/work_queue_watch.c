/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue_watch.h"

#include "hash_table.h"
#include "debug.h"
#include "link.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>

struct work_queue_watch {
	struct hash_table *table;
};

struct entry {
	time_t last_checked;
	int64_t size;
};

struct work_queue_watch * work_queue_watch_create()
{
	struct work_queue_watch *w = malloc(sizeof(*w));
	w->table = hash_table_create(0,0);
	return w;
}

void work_queue_watch_delete( struct work_queue_watch *w )
{
	char *path;
	struct entry *e;

	hash_table_firstkey(w->table);
	while(hash_table_nextkey(w->table,&path,(void**)&e)) {
		e = hash_table_remove(w->table,path);
		free(e);
		hash_table_firstkey(w->table);
	}
	hash_table_delete(w->table);
	free(w);
}


void work_queue_watch_add_file( struct work_queue_watch *w, const char *path )
{
	struct entry *e = malloc(sizeof(*e));
	memset(e,0,sizeof(*e));
	hash_table_insert(w->table,path,e);
}

void work_queue_watch_remove_file( struct work_queue_watch *w, const char *path )
{
	struct entry *e = hash_table_remove(w->table,path);
	if(e) free(e);
}

int work_queue_watch_check( struct work_queue_watch *w )
{
	char *path;
	struct entry *e;

	hash_table_firstkey(w->table);
	while(hash_table_nextkey(w->table,&path,(void**)&e)) {
		struct stat info;
		if(stat(path,&info)==0) {
			if(info.st_size > e->size) {
				return 1;
			}
		}
	}

	return 0;
}

int work_queue_watch_send_changes( struct work_queue_watch *w, struct link *master, time_t stoptime )
{
	char *path;
	struct entry *e;

	hash_table_firstkey(w->table);
	while(hash_table_nextkey(w->table,&path,(void**)&e)) {
		struct stat info;
		if(stat(path,&info)==0) {
			if(info.st_size>e->size) {
				int64_t change = info.st_size - e->size;
				debug(D_WQ,"%s increased from %"PRId64" to %"PRId64" bytes",path,e->size,(int64_t)info.st_size);
				int fd = open(path,O_RDONLY);
				if(fd<0) {
					debug(D_WQ,"unable to open %s: %s",path,strerror(errno));
					continue;
				}

				lseek(fd,e->size,SEEK_SET);
				link_putfstring(master,"update %s %"PRId64" %"PRId64"\n",stoptime,path,e->size,change);
				int actual = link_stream_from_fd(master,fd,change,stoptime);
				close(fd);
				if(actual!=change) return 0;
			}

		}
	}

	return 1;
}

