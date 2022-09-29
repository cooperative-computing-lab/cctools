/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_SYSDEPS_H
#define PFS_SYSDEPS_H

#define ALIGN(type,size)  ((size+((sizeof(type))-1))&(~((sizeof(type)-1))))

/*
In this file, we establish all of the definitions
and feature activations that are dependent upon
each system, with some explanation as to why they
are necessary.

Notice that we simply define our own private
versions of structures like stat, stat64, and so forth.
These are *not* the user level versions of these
structure, *nor* the kernel level version.  These are
the structures used at the kernel interface, which
are occasionally different than the other two, and
remarkably difficult to pull in a definition from
the right include files.  So, we just define our own.
*/

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "pfs_types.h"

#include <sched.h>
#include <linux/net.h>

/*
Sadly, it is necessary to prefix each of the structure
tags with pfs (i.e. pfs_st_dev instead of st_dev) because
the structure tags themselves are frequently the target
of a macro definition that munges something at the user
level that does not concern us.
*/

struct pfs_kernel_timespec {
	uint64_t tv_sec;
	uint64_t tv_nsec;
};

struct pfs_kernel_stat {
	uint64_t st_dev;
	uint64_t st_ino;
	uint64_t st_nlink;
	uint32_t st_mode;
	uint32_t st_uid;
	uint32_t st_gid;
	uint32_t unused0;
	uint64_t st_rdev;
	int64_t  st_size;
	int64_t  st_blksize;
	int64_t  st_blocks;
#if !defined(st_atime)
		int32_t st_atime;
		int32_t st_atime_nsec;
		int32_t st_mtime;
		int32_t st_mtime_nsec;
		int32_t st_ctime;
		int32_t st_ctime_nsec;
#else
		struct pfs_kernel_timespec st_atim;
		struct pfs_kernel_timespec st_mtim;
		struct pfs_kernel_timespec st_ctim;
#endif
	int64_t unused1;
	int64_t unused2;
	int64_t unused3;
};

struct pfs_kernel_statfs {
		int64_t f_type;
		int64_t f_bsize;
		int64_t f_blocks;
		int64_t f_bfree;
		int64_t f_bavail;
		int64_t f_files;
		int64_t f_ffree;
	int64_t f_fsid;
		int64_t f_namelen;
		int64_t f_spare[6];
};

struct pfs_kernel_iovec {
	void     *iov_base;
	uint64_t  iov_len;
};

/*
Note that the typical libc sigaction places the
sa_mask field as the second value. This is hard to expand,
as the number of signals tends to increase.  The kernel
defined sigaction puts sa_mask last as follows:
*/

struct pfs_kernel_sigaction {
	uint64_t pfs_sa_handler;
	int64_t  pfs_sa_flags;
	uint64_t pfs_sa_restorer;
	uint8_t  pfs_sa_mask[128];
};

#ifndef CLONE_PTRACE
#define CLONE_PTRACE    0x00002000
#endif

#ifndef CLONE_PARENT
#define CLONE_PARENT    0x00008000
#endif

#define PFS_GETLK	5
#define PFS_SETLK	6
#define PFS_SETLKW	7

#define PFS_TIOCGPGRP   0x540F


#endif

/* vim: set noexpandtab tabstop=4: */
