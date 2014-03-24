/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_MMAP_H
#define PFS_MMAP_H

#include <sys/mman.h>

#include <assert.h>
#include <stdlib.h>

#include "pfs_channel.h"
#include "pfs_types.h"
#include "pfs_file.h"

class pfs_mmap {
public:
	pfs_mmap( pfs_file *_file, pfs_size_t _logical_addr, pfs_size_t _channel_offset, pfs_size_t _map_length, pfs_size_t _file_offset, int _prot, int _flags )
	{
		if(_flags&MAP_SHARED && _prot&PROT_WRITE)
			file = _file; /* we only keep the reference if we need to write back */
		else
			file = NULL;
		logical_addr = _logical_addr;
		channel_offset = _channel_offset;
		map_length = _map_length;
		file_offset = _file_offset;
		prot = _prot;
		flags = _flags;
		if(file)
			file->addref();
		pfs_channel_addref(channel_offset);
	}

	pfs_mmap( pfs_mmap * m ) {
		file = m->file;
		logical_addr = m->logical_addr;
		channel_offset = m->channel_offset;
		map_length = m->map_length;
		file_offset = m->file_offset;
		prot = m->prot;
		flags = m->flags;
		if(file)
			file->addref();
		pfs_channel_addref(channel_offset);
	}

	~pfs_mmap()
	{
		if(file) {
			file->delref();
			if(file->refs()<1) {
				file->close();
				delete file;
			}
		}
		pfs_channel_free(channel_offset);
	}

	pfs_file   *file;
	pfs_size_t logical_addr;
	pfs_size_t channel_offset;
	pfs_size_t map_length;
	pfs_size_t file_offset;
	int	   prot;
	int	   flags;
	pfs_mmap   *next;
};

#endif

/* vim: set noexpandtab tabstop=4: */
