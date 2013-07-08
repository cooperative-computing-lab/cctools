/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_DIR_H
#define PFS_DIR_H

#include "pfs_sysdeps.h"
#include "pfs_types.h"
#include "pfs_service.h"
#include "pfs_file.h"

class pfs_dir : public pfs_file {
public:
	pfs_dir( pfs_name *n );
	virtual ~pfs_dir();

	virtual int fstat( struct pfs_stat *buf );
	virtual int fstatfs( struct pfs_statfs *buf );
	virtual int fchmod( mode_t mode );
	virtual int fchown( uid_t uid, gid_t gid );

	virtual int append( const char *name );
	virtual struct dirent * fdreaddir( pfs_off_t offset, pfs_off_t *next_offset );

	virtual int is_seekable();

private:
	char *data;
	pfs_off_t length;
	pfs_off_t maxlength;
	int total_iterations;
};

#endif
