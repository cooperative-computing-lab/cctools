/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#define _POSIX_MAPPED_FILES
#define POSIX_SYNCHRONIZED_IO


#include "pfs_channel_cache.h"
#include "pfs_channel.h"
#include "debug.h"
#include "pfs_sys.h"

#include "hash_table.h"
#include "macros.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/mman.h>

static struct hash_table * table=0;

struct entry {
	char *name;
	pfs_size_t length;
	pfs_size_t start;
	int dirty;
	int numrefs;
};

static struct entry * entry_create( const char *name, pfs_size_t length, pfs_size_t start )
{
	struct entry *e = malloc(sizeof(*e));
	if(!e) return 0;

	e->name = strdup(name);
	if(!e->name) {
		free(e);
		return 0;
	}

	e->length = length;
	e->start = start;
	e->dirty = 0;
	e->numrefs = 1;

	return e;
}

static void entry_delete( struct entry *e )
{
	free(e->name);
	free(e);
}

static void entry_deref( struct entry *e )
{
	e->numrefs--;
	debug(D_CHANNEL,"deref %s start 0x%llx length 0x%llx",e->name);
	if(e->numrefs<=0) {
		debug(D_CHANNEL,"removed %s start 0x%llx length 0x%llx",e->name,e->start,e->length);
		hash_table_remove(table,e->name);
		pfs_channel_free(e->start);
		entry_delete(e);
	}
}

static int load_file( const char *name, int fd, pfs_size_t length, pfs_size_t start, pfs_size_t blocksize )
{
	pfs_size_t data_left = length;
	pfs_size_t offset = 0;
	pfs_size_t chunk, actual;

	debug(D_CHANNEL,"loading: %s",name);

	while(data_left>0) {
		chunk = MIN(data_left,blocksize);
		actual = pfs_pread(fd,pfs_channel_base()+start+offset,chunk,offset);
		if(actual>0) {
			offset += actual;
			data_left -= actual;
		} else if(actual==0) {
			memset(pfs_channel_base()+start+offset,0,data_left);
			offset += data_left;
			data_left = 0;
		} else {
			break;
		}
	}

	if(data_left) {
		debug(D_CHANNEL,"loading: failed: %s",strerror(errno));
		return 0;
	} else {
		/*
		we must invalidate the others' mapping of this file,
		otherwise, they will see old data that was in this place.
		*/
		msync(pfs_channel_base()+start,length,MS_INVALIDATE);
		return 1;
	}
}

int pfs_channel_cache_alloc( const char *name, int fd, pfs_size_t *length, pfs_size_t *start )
{
	struct entry *e;
	struct pfs_stat buf;
	int result;

	if(!table) {
		table = hash_table_create(0,0);
		if(!table) return 0;
	}

	e = hash_table_lookup(table,name);
	if(e) {
		*start = e->start;
		*length = e->length;
		e->numrefs++;
		debug(D_CHANNEL,"addref %s start 0x%llx length 0x%llx",name,e->start,e->length);
		return 1;
	} else {
		result = pfs_fstat(fd,&buf);
		if(result!=0) return 0;

		*length = buf.st_size;

		if(pfs_channel_alloc(*length,start)) {
			if(load_file(name,fd,*length,*start,buf.st_blksize)) {
				e = entry_create(name,*length,*start);
				if(e) {
					if(hash_table_insert(table,name,e)) {
						debug(D_CHANNEL,"added %s start 0x%llx length 0x%llx",name,*start,*length);
						return 1;
					}
					entry_delete(e);
				}
			}
			pfs_channel_free(*start);
		}
	}

	return 0;
}

int pfs_channel_cache_freename( const char *name )
{
	struct entry *e;
	e = hash_table_lookup(table,name);
	if(e) {
		entry_deref(e);
		return 1;
	} else {
		return 0;
	}
}

void  pfs_channel_cache_freeaddr( pfs_size_t start, pfs_size_t length )
{
	char *key;
	struct entry *e;

	hash_table_firstkey(table);
	while(hash_table_nextkey(table,&key,(void**)&e)) {
		if(e->start==start && e->length==length) {
			entry_deref(e);
			break;
		}
	}
}

int   pfs_channel_cache_make_dirty( const char *name )
{
	struct entry *e;
	e = hash_table_lookup(table,name);
	if(e) {
		e->dirty = 1;
		return 1;
	} else {
		return 0;
	}
}

int   pfs_channel_cache_is_dirty( const char *name )
{
	struct entry *e;
	e = hash_table_lookup(table,name);
	if(e) {
		return e->dirty;
	} else {
		return 0;
	}
}

int   pfs_channel_cache_refs( const char *name )
{
	struct entry *e;
	e = hash_table_lookup(table,name);
	if(e) {
		return e->numrefs;
	} else {
		return 0;
	}
}

int pfs_channel_cache_start( const char *name, pfs_size_t *start )
{
	struct entry *e;
	e = hash_table_lookup(table,name);
	if(e) {
		*start = e->start;
		return 1;
	} else {
		return 0;
	}
}

