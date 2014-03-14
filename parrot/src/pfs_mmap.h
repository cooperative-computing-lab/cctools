/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_MMAP_H
#define PFS_MMAP_H

#include <assert.h>

#include "pfs_channel.h"
#include "pfs_types.h"
#include "pfs_file.h"

class pfs_mmap {
public:
	pfs_mmap( pfs_file *_file, pfs_size_t _logical_addr, pfs_size_t _channel_offset, pfs_size_t _map_length, pfs_size_t _file_offset, int _prot, int _flags )
	{
		file = _file;
		logical_addr = _logical_addr;
		channel_offset = _channel_offset;
		map_length = _map_length;
		file_offset = _file_offset;
		prot = _prot;
		flags = _flags;
		file->addref();
	}

	pfs_mmap( pfs_mmap * m ) {
		file = m->file;
		logical_addr = m->logical_addr;
		pfs_channel_lookup(file->get_name()->path,&channel_offset); /* increase refcount */
		assert(channel_offset == m->channel_offset);
		map_length = m->map_length;
		file_offset = m->file_offset;
		prot = m->prot;
		flags = m->flags;
		file->addref();
	}

	~pfs_mmap()
	{
		file->delref();
		if(file->refs()<1) {
			file->close();
			delete file;
		}
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
