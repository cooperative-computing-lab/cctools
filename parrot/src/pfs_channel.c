/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_channel.h"

#include "xxmalloc.h"
#include "debug.h"

#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <bits/mman.h>
#include <string.h>
#include <errno.h>

extern char pfs_temp_dir[];

struct entry {
	char *name;
	pfs_size_t start;
	pfs_size_t length;
	int inuse;
	struct entry *prev;
	struct entry *next;
};

#define CHANNEL_FMT "`%s':%" PRIx64 ":%zu"
#define CHANNEL_FMT_ARGS(e) e->name, (uint64_t)e->start, (size_t)e->length

static struct entry *head=0;
static int channel_fd=-1;
static char *channel_base=0;
static pfs_size_t channel_size;

static struct entry * entry_create( const char *name, pfs_size_t start, pfs_size_t length, struct entry *prev, struct entry *next )
{
	struct entry *e;

	e = malloc(sizeof(*e));
	if(!e) return 0;

	e->name = name ? xxstrdup(name) : NULL;

	e->start = start;
	e->length = length;
	e->inuse = 0;

	if(prev) {
		e->prev = prev;
		prev->next = e;
	} else {
		e->prev = e;
	}

	if(next) {
		e->next = next;
		next->prev = e;
	} else {
		e->next = e;
	}

	return e;
}

static void entry_delete( struct entry *e )
{
	if(head==e) head = e->next;
	if(e->prev) e->prev->next = e->next;
	if(e->next) e->next->prev = e->prev;
	free(e->name);
	free(e);
}

static int channel_create (const char *dir)
{
	int fd;
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/parrot-channel.XXXXXX", dir);

	/* FIXME Future Work: use memfd_create for channel. */

	debug(D_DEBUG, "trying to create channel '%s'", path);
	fd = mkstemp(path);
	if (fd < 0) {
		debug(D_DEBUG, "could not create channel: %s", strerror(errno));
		return -1;
	}
	unlink(path);

	/* test if we can use it for executable data (i.e. is dir on a file system mounted with the 'noexec' option) */
	{
		size_t l = getpagesize();
		if (ftruncate(fd, l) == -1) {
			debug(D_DEBUG, "could not grow channel: %s", strerror(errno));
			close(fd);
			return errno = EINVAL, -1;
		}
		void *addr = mmap(NULL, l, PROT_READ|PROT_EXEC, MAP_SHARED, fd, 0);
		if (addr == MAP_FAILED) {
			debug(D_DEBUG, "failed executable mapping: %s", strerror(errno));
			close(fd);
			return errno = EINVAL, -1;
		}
		munmap(addr, l);
		ftruncate(fd, 0);
	}

	return fd;
}

int pfs_channel_init( pfs_size_t size )
{
	/* We try to use /dev/shm for the channel because we rely on POSIX
	 * semantics for mmap. Some distributed file systems like GPFS do not
	 * handle this correctly.
	 *
	 * See: https://github.com/cooperative-computing-lab/cctools/issues/305
	 */
	const char *channel_dirs[] = {
		"/dev/shm",
		"/tmp",
		"/var/tmp",
		pfs_temp_dir,
	};
	int i;

	close(channel_fd);
	channel_fd = -1;

	for (i = 0; i < (int)(sizeof(channel_dirs)/sizeof(channel_dirs[0])); i++) {
		channel_fd = channel_create(channel_dirs[i]);
		if (channel_fd >= 0)
			break;
	}
	if (channel_fd < 0) {
		fatal("could not create a channel!");
	}

	channel_size = size;
	ftruncate(channel_fd,channel_size);

	channel_base = (char*) mmap(0,channel_size,PROT_READ|PROT_WRITE,MAP_SHARED,channel_fd,0);
	if(channel_base==MAP_FAILED) {
		close(channel_fd);
		channel_fd = -1;
		return 0;
	}

	head = entry_create(0,0,channel_size,0,0);
	if(!head) {
		close(channel_fd);
		channel_fd = -1;
		munmap(channel_base,channel_size);
		return 0;
	}

	debug(D_CHANNEL,"fd is %d",channel_fd);

	return 1;
}

int pfs_channel_fd()
{
	return channel_fd;
}

char * pfs_channel_base()
{
	return channel_base;
}

static int page_size=0;

static pfs_size_t round_up( pfs_size_t x )
{
	if(page_size==0) page_size = sysconf(_SC_PAGE_SIZE);
	if(x%page_size) x = page_size * ((x/page_size)+1);
	if(x<=0) x=page_size;
	return x;
}

int pfs_channel_alloc( const char *name, pfs_size_t length, pfs_size_t *start )
{
	struct entry *e = head;
	struct entry *tail;
	pfs_size_t newsize;

	length = round_up(length);

	do {
		if(!e->inuse) {
			if(e->length>=length) {
				if(e->length>length) {
					struct entry *f;
					f = entry_create(0,e->start+length,e->length-length,e,e->next);
					if(!f) return 0;
				}

				e->name = name ? xxstrdup(name) : 0;
				e->length = length;
				e->inuse = 1;
				*start = e->start;
				memset(channel_base+*start+length-page_size,0,page_size);
				debug(D_DEBUG, "allocated channel " CHANNEL_FMT, CHANNEL_FMT_ARGS(e));
				return 1;
			} else {
				/* not big enough */
			}
		}
		tail = e;
		e = e->next;
	} while(e!=head);

	debug(D_CHANNEL,"channel is full, attempting to expand it...");
	newsize = channel_size + length;

	if(ftruncate64(channel_fd,newsize)==0) {
		void *newbase;
		newbase = mremap(channel_base,channel_size,newsize,MREMAP_MAYMOVE);
		if(newbase!=MAP_FAILED) {
			e = entry_create(name,channel_size,newsize-channel_size,tail,head);
			channel_size = newsize;
			channel_base = newbase;
			debug(D_CHANNEL,"channel expanded to 0x%" PRIu64 " bytes at base 0x%" PRIdPTR, newsize, (uintptr_t) newbase);
			return pfs_channel_alloc(name,length,start);
		}
		ftruncate64(channel_fd,channel_size);
	}

	debug(D_CHANNEL|D_NOTICE,"out of channel space: %s",strerror(errno));

	return 0;
}

int pfs_channel_lookup( const char *name, pfs_size_t *start )
{
	struct entry *e = head;

	do {
		if(e->name && !strcmp(e->name,name)) {
			*start = e->start;
			return 1;
		}
		e = e->next;
	} while(e!=head);

	return 0;
}

int pfs_channel_addref( pfs_size_t start )
{
	struct entry *e = head;

	do {
		if(e->start==start) {
			e->inuse++;
			debug(D_DEBUG, "increasing refcount to %d for channel " CHANNEL_FMT, e->inuse, CHANNEL_FMT_ARGS(e));
			return 1;
		}
		e = e->next;
	} while(e!=head);
	return 0;
}

int pfs_channel_update_name( const char *oldname, const char *newname )
{
	struct entry *e = head;
	debug(D_CHANNEL,"updating channel for file '%s' to '%s'",oldname,newname);
	do {
		if(e->name && !strcmp(e->name,oldname)) {
			free(e->name);
			e->name = newname ? xxstrdup(newname) : 0;
			debug(D_DEBUG, "channel is now " CHANNEL_FMT, CHANNEL_FMT_ARGS(e));
			return 1;
		}
		e = e->next;
	} while(e!=head);

	return 0;
}

void pfs_channel_free( pfs_size_t start )
{
	struct entry *e = head;

	do {
		if(e->start==start) {
			e->inuse--;
			debug(D_DEBUG, "decreasing refcount to %d for channel " CHANNEL_FMT, e->inuse, CHANNEL_FMT_ARGS(e));
			if(e->inuse<=0) {
				struct entry *prev = e->prev;
				struct entry *next = e->next;

				debug(D_DEBUG, "freeing channel " CHANNEL_FMT, CHANNEL_FMT_ARGS(e));
				e->inuse = 0;
				/* collapse adjacent free blocks */
				if((e->start <= next->start) && next->inuse == 0) {
					e->length += next->length;
					entry_delete(next);
					if (next == prev)
						prev = NULL;
				}
				if(prev && (prev->start <= e->start) && prev->inuse == 0) {
					prev->length += e->length;
					entry_delete(e);
				}
			}
			return;
		}
		e = e->next;
	} while(e!=head);
}

/* vim: set noexpandtab tabstop=4: */
