/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_TYPES_H
#define PFS_TYPES_H

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <sys/wait.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <dirent.h>
#include <sys/stat.h>

#define PFS_PATH_MAX 1024
#define PFS_LINE_MAX 1024
#define PFS_ARG_MAX  1024

typedef int64_t pfs_ssize_t;
typedef int64_t pfs_size_t;
typedef int64_t pfs_off_t;

#define PFS_SIZE_FORMAT "lld"

struct pfs_stat {
	int64_t	st_dev;
	int64_t	st_ino;
	int64_t	st_mode;
	int64_t	st_nlink;
	int64_t	st_uid;
	int64_t	st_gid;
	int64_t	st_rdev;
	int64_t	st_size;
	int64_t	st_blksize;
	int64_t	st_blocks;
#if !defined(st_atime)
	int64_t st_atime;
	int64_t st_mtime;
	int64_t st_ctime;
#else
	struct timespec st_atim;
	struct timespec st_mtim;
	struct timespec st_ctim;
#endif
};

struct pfs_statfs {
	int64_t	f_type;
	int64_t f_blocks;
	int64_t f_bavail;
	int64_t f_bsize;
	int64_t f_bfree;
	int64_t f_files;
	int64_t f_ffree;
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
