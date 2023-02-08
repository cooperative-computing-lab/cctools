/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
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
	UINT64_T tv_sec;
	UINT64_T tv_nsec;
};

struct pfs_kernel_stat {
	UINT64_T st_dev;
	UINT64_T st_ino;
	UINT64_T st_nlink;
	UINT32_T st_mode;
	UINT32_T st_uid;
	UINT32_T st_gid;
	UINT32_T unused0;
	UINT64_T st_rdev;
	INT64_T  st_size;
	INT64_T  st_blksize;
	INT64_T  st_blocks;
#if !defined(st_atime)
		INT32_T st_atime;
		INT32_T st_atime_nsec;
		INT32_T st_mtime;
		INT32_T st_mtime_nsec;
		INT32_T st_ctime;
		INT32_T st_ctime_nsec;
#else
		struct pfs_kernel_timespec st_atim;
		struct pfs_kernel_timespec st_mtim;
		struct pfs_kernel_timespec st_ctim;
#endif
	INT64_T unused1;
	INT64_T unused2;
	INT64_T unused3;
};

struct pfs_kernel_statfs {
		INT64_T f_type;
		INT64_T f_bsize;
		INT64_T f_blocks;
		INT64_T f_bfree;
		INT64_T f_bavail;
		INT64_T f_files;
		INT64_T f_ffree;
	INT64_T f_fsid;
		INT64_T f_namelen;
		INT64_T f_spare[6];
};

struct pfs_kernel_statx_timestamp {
	INT64_T tv_sec;
	UINT32_T tv_nsec;
};

struct pfs_kernel_statx {
	UINT32_T stx_mask;
	UINT32_T stx_blksize;
	UINT64_T stx_attributes;
	UINT32_T stx_nlink;
	UINT32_T stx_uid;
	UINT32_T stx_gid;
	UINT16_T stx_mode;
	UINT64_T stx_ino;
	UINT64_T stx_size;
	UINT64_T stx_blocks;
	UINT64_T stx_attributes_mask;

	struct pfs_kernel_statx_timestamp stx_atime;
	struct pfs_kernel_statx_timestamp stx_btime;
	struct pfs_kernel_statx_timestamp stx_ctime;
	struct pfs_kernel_statx_timestamp stx_mtime;

	UINT32_T stx_rdev_major;
	UINT32_T stx_rdev_minor;

	UINT32_T stx_dev_major;
	UINT32_T stx_dev_minor;
};

struct pfs_kernel_iovec {
	void     *iov_base;
	UINT64_T  iov_len;
};

/*
Note that the typical libc sigaction places the
sa_mask field as the second value. This is hard to expand,
as the number of signals tends to increase.  The kernel
defined sigaction puts sa_mask last as follows:
*/

struct pfs_kernel_sigaction {
	UINT64_T pfs_sa_handler;
	INT64_T  pfs_sa_flags;
	UINT64_T pfs_sa_restorer;
	UINT8_T  pfs_sa_mask[128];
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
