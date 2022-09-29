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
#include <inttypes.h>

/*
On Linux, fork() does not cause the child process to
inherit the ptrace attribute.  Instead, we must
convert fork() into an equivalent clone() with the
ptrace attribute.  Further, the necessary flags
are not always defined in all kernels.
*/

#define PFS_CONVERT_FORK_TO_CLONE 1

/*
Sadly, it is necessary to prefix each of the structure
tags with pfs (i.e. pfs_st_dev instead of st_dev) because
the structure tags themselves are frequently the target
of a macro definition that munges something at the user
level that does not concern us.
*/

struct pfs_kernel_timespec {
	uint32_t tv_sec;
	uint32_t tv_nsec;
} __attribute__((packed));

struct pfs_kernel_timeval {
	uint32_t tv_sec;
	uint32_t tv_usec;
} __attribute__((packed));

struct pfs_kernel_utimbuf {
	uint32_t actime;
	uint32_t modtime;
} __attribute__((packed));

struct pfs_kernel_rusage {
	struct pfs_kernel_timeval ru_utime;
	struct pfs_kernel_timeval ru_stime;
	int32_t ru_maxrss;
	int32_t ru_ixrss;
	int32_t ru_idrss;
	int32_t ru_isrss;
	int32_t ru_minflt;
	int32_t ru_majflt;
	int32_t ru_nswap;
	int32_t ru_inblock;
	int32_t ru_oublock;
	int32_t ru_msgsnd;
	int32_t ru_msgrcv;
	int32_t ru_nsignals;
	int32_t ru_nvcsw;
	int32_t ru_nivcsw;
} __attribute__((packed));

struct pfs_kernel_stat {
	uint32_t st_dev;
	uint32_t st_ino;
	uint16_t st_mode;
	uint16_t st_nlink;
	uint16_t st_uid;
	uint16_t st_gid;
	uint32_t st_rdev;
	uint32_t st_size;
	uint32_t st_blksize;
	uint32_t st_blocks;
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
	uint32_t st_p4;
	uint32_t st_p5;
} __attribute__((packed));

struct pfs_kernel_stat64 {
	uint64_t st_dev;
	uint32_t st_pad1;
	uint32_t st_ino;
	uint32_t st_mode;
	uint32_t st_nlink;
	uint32_t st_uid;
	uint32_t st_gid;
	uint64_t st_rdev;
	uint32_t st_pad2;
	int64_t st_size;
	uint32_t st_blksize;
	uint64_t st_blocks;
#if !defined(st_atime)
		uint32_t st_atime;
		uint32_t st_atime_nsec;
		uint32_t st_mtime;
		uint32_t st_mtime_nsec;
		uint32_t st_ctime;
		uint32_t st_ctime_nsec;
#else
		struct pfs_kernel_timespec st_atim;
		struct pfs_kernel_timespec st_mtim;
		struct pfs_kernel_timespec st_ctim;
#endif
	uint64_t st_ino_extra;
} __attribute__((packed));

struct pfs_kernel_statfs {
		uint32_t f_type;
		uint32_t f_bsize;
		uint32_t f_blocks;
		uint32_t f_bfree;
		uint32_t f_bavail;
		uint32_t f_files;
		uint32_t f_ffree;
	uint64_t f_fsid;
		uint32_t f_namelen;
		uint32_t f_spare[6];
} __attribute__((packed));

struct pfs_kernel_statfs64 {
		uint32_t f_type;
		uint32_t f_bsize;
		uint64_t f_blocks;
		uint64_t f_bfree;
		uint64_t f_bavail;
		uint64_t f_files;
		uint64_t f_ffree;
		uint64_t f_fsid;
		uint32_t f_namelen;
		uint32_t f_frsize;
		uint32_t f_spare[5];
} __attribute__((packed));

struct pfs_kernel_iovec {
	uint32_t iov_base;
	uint32_t iov_len;
} __attribute__((packed));

struct pfs_kernel_msghdr {
	uint32_t msg_name;
	uint32_t msg_namelen;
	uint32_t msg_iov;
	uint32_t msg_iovlen;
	uint32_t msg_control;
	uint32_t msg_controllen;
	uint32_t msg_flags;
} __attribute__((packed));

struct pfs_kernel_cmsghdr {
	uint32_t cmsg_len;
	int32_t cmsg_level;
	int32_t cmsg_type;
} __attribute__((packed));

struct pfs_kernel_sockaddr_un {
	uint16_t sun_family;
	char sun_path[108];
} __attribute__((packed));

struct pfs_kernel_sigaction {
	uint32_t pfs_sa_handler;
	uint32_t pfs_sa_flags;
	uint32_t pfs_sa_restorer;
	uint8_t  pfs_sa_mask[128];
} __attribute__((packed));

struct pfs_kernel_ifconf {
	uint32_t ifc_len;
	uint32_t ifc_buffer;
} __attribute__ ((packed));

struct pfs_kernel_flock {
	uint16_t l_type;
	uint16_t l_whence;
	uint32_t l_start;
	uint32_t l_len;
	uint32_t l_pid;
}__attribute__ ((packed));

struct pfs_kernel_flock64 {
	uint16_t l_type;
	uint16_t l_whence;
	uint64_t l_start;
	uint64_t l_len;
	uint32_t l_pid;
}__attribute__ ((packed));

#ifndef CLONE_PTRACE
#define CLONE_PTRACE    0x00002000
#endif

#ifndef CLONE_PARENT
#define CLONE_PARENT    0x00008000
#endif

#define PFS_GETLK	5
#define PFS_SETLK	6
#define PFS_SETLKW	7

#define PFS_GETLK64	12
#define PFS_SETLK64	13
#define PFS_SETLKW64	14

#define PFS_TIOCGPGRP   0x540F

#define COPY_RUSAGE( s, t ) \
	t.ru_utime = s.ru_utime;\
	t.ru_utime = s.ru_stime;\
	t.ru_maxrss = s.ru_maxrss;\
		t.ru_ixrss = s.ru_ixrss;\
		t.ru_idrss = s.ru_idrss;\
		t.ru_isrss = s.ru_isrss;\
		t.ru_minflt = s.ru_minflt;\
		t.ru_majflt = s.ru_majflt;\
		t.ru_nswap = s.ru_nswap;\
		t.ru_inblock = s.ru_inblock;\
		t.ru_oublock = s.ru_oublock;\
		t.ru_msgsnd = s.ru_msgsnd;\
		t.ru_msgrcv = s.ru_msgrcv;\
		t.ru_nsignals = s.ru_nsignals;\
		t.ru_nvcsw = s.ru_nvcsw;\
		t.ru_nivcsw = s.ru_nivcsw;

#define COPY_FLOCK( s, t ) \
	t.l_type = s.l_type;\
	t.l_start = s.l_start;\
	t.l_whence = s.l_whence;\
	t.l_len = s.l_len;\
	t.l_pid = s.l_pid;

#define COPY_TIMEVAL( s, t ) \
	t.tv_sec = s.tv_sec;\
	t.tv_usec = s.tv_usec;

#define COPY_UTIMBUF( s, t ) \
	t.actime = s.actime;\
	t.modtime = s.modtime;

#endif

/* vim: set noexpandtab tabstop=4: */
