/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_channel.h"
#include "debug.h"

#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <bits/mman.h>
#include <string.h>
#include <errno.h>

extern char pfs_temp_dir[];

struct entry {
	pfs_size_t start;
	pfs_size_t length;
	int inuse;
	struct entry *prev;
	struct entry *next;
};

static struct entry *head=0;
static int channel_fd=-1;
static char *channel_base=0;
static pfs_size_t channel_size;

static struct entry * entry_create( pfs_size_t start, pfs_size_t length, struct entry *prev, struct entry *next )
{
	struct entry *e;

	e = malloc(sizeof(*e));
	if(!e) return 0;

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
	free(e);
}

int pfs_channel_init( pfs_size_t size )
{
	char path[PATH_MAX];
	int fd;

	sprintf(path,"%s/pfs.tmp.XXXXXX",pfs_temp_dir);
	channel_fd = mkstemp(path);
	if(channel_fd<0) return 0;
	unlink(path);
	ftruncate(channel_fd,size);

	channel_size = size;

	channel_base = (char*) mmap(0,size,PROT_READ|PROT_WRITE,MAP_SHARED,channel_fd,0);
	if(channel_base==MAP_FAILED) {
		close(fd);
		return 0;
	}

	head = entry_create(0,size,0,0);
	if(!head) {
		close(fd);
		munmap(channel_base,size);
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

int pfs_channel_alloc( pfs_size_t length, pfs_size_t *start )
{
	struct entry *e = head;
	struct entry *tail;
	pfs_size_t newsize;

	length = round_up(length);

	do {
		if(!e->inuse) {
			if(e->length==length) {
				e->inuse = 1;
				*start = e->start;
				memset(channel_base+*start+length-page_size,0,page_size);
				return 1;
			} else if(e->length>length) {
				struct entry *f;
				f = entry_create(e->start+length,e->length-length,e,e->next);
				if(!f) return 0;

				e->length = length;
				e->inuse = 1;
				*start = e->start;
				memset(channel_base+*start+length-page_size,0,page_size);
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
			e = entry_create(channel_size,newsize-channel_size,tail,head);
			channel_size = newsize;
			channel_base = newbase;
			debug(D_CHANNEL,"channel expanded to 0x%x bytes at base 0x%x",(PTRINT_T)newsize,newbase);
			return pfs_channel_alloc(length,start);
		}
		ftruncate64(channel_fd,channel_size);
	}

	debug(D_CHANNEL|D_NOTICE,"out of channel space: %s",strerror(errno));

	return 0;
}

void pfs_channel_free( pfs_size_t start )
{
	struct entry *e = head;

	do {
		if(e->start==start) {
			e->inuse=0;
			if( (e->prev->start < e->start) && !e->prev->inuse) {
				struct entry *f = e->prev;
				f->length += e->length;
				entry_delete(e);
				e = f;
			}
			if( (e->next->start>e->start) && !e->next->inuse) {
				struct entry *f = e->next;
				f->start = e->start;
				f->length += e->length;
				entry_delete(e);
				e = f;
			}
			return;
		}
		e = e->next;
	} while(e!=head);
}


