/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef PFS_TYPES_H
#define PFS_TYPES_H

#include "int_sizes.h"

#include <sys/wait.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <dirent.h>
#include <sys/stat.h>

#define PFS_PATH_MAX 1024
#define PFS_LINE_MAX 1024
#define PFS_ARG_MAX  1024

typedef INT64_T pfs_ssize_t;
typedef INT64_T pfs_size_t;
typedef INT64_T pfs_off_t;

struct pfs_stat {
	INT64_T	st_dev;
	INT64_T	st_ino;
	INT64_T	st_mode;
	INT64_T	st_nlink;
	INT64_T	st_uid;
	INT64_T	st_gid;
	INT64_T	st_rdev;
	INT64_T	st_size;
	INT64_T	st_blksize;
	INT64_T	st_blocks;
#if !defined(st_atime)
	INT64_T st_atime;
	INT64_T st_mtime;
	INT64_T st_ctime;
#else
	struct timespec st_atim;
	struct timespec st_mtim;
	struct timespec st_ctim;
#endif
};

struct pfs_statfs {
	INT64_T	f_type;
	INT64_T f_blocks;
	INT64_T f_bavail;
	INT64_T f_bsize;
	INT64_T f_bfree;
	INT64_T f_files;
	INT64_T f_ffree;
};

extern uid_t pfs_uid;
extern gid_t pfs_gid;

#ifndef COPY_STAT
#define COPY_STAT( a, b )\
	memset(&(b),0,sizeof(b));\
	(b).st_dev = (a).st_dev;\
	(b).st_ino = (a).st_ino;\
	(b).st_mode = (a).st_mode;\
	(b).st_nlink = (a).st_nlink;\
	(b).st_uid = (a).st_uid;\
	(b).st_gid = (a).st_gid;\
	(b).st_rdev = (a).st_rdev;\
	(b).st_size = (a).st_size;\
	(b).st_blksize = (a).st_blksize;\
	(b).st_blocks = (a).st_blocks;\
	(b).st_atime = (a).st_atime;\
	(b).st_mtime = (a).st_mtime;\
	(b).st_ctime = (a).st_ctime;
#endif

#ifndef COPY_CSTAT
#define COPY_CSTAT( a, b )\
	memset(&(b),0,sizeof(b));\
	(b).st_dev = (a).cst_dev;\
	(b).st_ino = (a).cst_ino;\
	(b).st_mode = (a).cst_mode;\
	(b).st_nlink = (a).cst_nlink;\
	(b).st_uid = pfs_uid;\
	(b).st_gid = pfs_gid;\
	(b).st_rdev = (a).cst_rdev;\
	(b).st_size = (a).cst_size;\
	(b).st_blksize = (a).cst_blksize;\
	(b).st_blocks = (a).cst_blocks;\
	(b).st_atime = (a).cst_atime;\
	(b).st_mtime = (a).cst_mtime;\
	(b).st_ctime = (a).cst_ctime;
#endif

#ifndef COPY_STATFS
#define COPY_STATFS( a, b )\
	memset(&(b),0,sizeof(b));\
	(b).f_type = (a).f_type;\
	(b).f_blocks = (a).f_blocks;\
	(b).f_bavail = (a).f_bavail;\
	(b).f_bsize = (a).f_bsize;\
	(b).f_bfree = (a).f_bfree;\
	(b).f_files = (a).f_files;\
	(b).f_ffree = (a).f_ffree;
#endif

#endif
