/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef PFS_FILE_GZIP_H
#define PFS_FILE_GZIP_H

#include "pfs_file.h"

pfs_file * pfs_gzip_open( pfs_file *file, int flags, int mode );

#endif
