/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef PFS_NAME_H
#define PFS_NAME_H

#include "pfs_types.h"

class pfs_service;

struct pfs_name {
	pfs_service *service;
	char logical_name[PFS_PATH_MAX];
        char service_name[PFS_PATH_MAX];
	char path[PFS_PATH_MAX];
	char host[PFS_PATH_MAX];
	char hostport[PFS_PATH_MAX];
	int  port;
	char rest[PFS_PATH_MAX];
	int  is_local;
};

#endif
