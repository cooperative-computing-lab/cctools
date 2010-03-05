/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_SYSDEPS_H
#define PFS_SYSDEPS_H

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
	UINT32_T tv_sec;
	UINT32_T tv_nsec;
} __attribute__((packed));

struct pfs_kernel_timeval {
	UINT32_T tv_sec;
	UINT32_T tv_usec;
} __attribute__((packed));

struct pfs_kernel_utimbuf {
	UINT32_T actime;
	UINT32_T modtime;
} __attribute__((packed));

struct pfs_kernel_rusage {
	struct pfs_kernel_timeval ru_utime;
	struct pfs_kernel_timeval ru_stime;
 	INT32_T ru_maxrss;
 	INT32_T ru_ixrss;
 	INT32_T ru_idrss;
 	INT32_T ru_isrss;
 	INT32_T ru_minflt;
 	INT32_T ru_majflt;
 	INT32_T ru_nswap;
 	INT32_T ru_inblock;
 	INT32_T ru_oublock;
 	INT32_T ru_msgsnd;
 	INT32_T ru_msgrcv;
 	INT32_T ru_nsignals;
 	INT32_T ru_nvcsw;
 	INT32_T ru_nivcsw;
} __attribute__((packed));

struct pfs_kernel_stat {
	UINT32_T st_dev;
	UINT32_T st_ino;
	UINT16_T st_mode;
	UINT16_T st_nlink;
	UINT16_T st_uid;
	UINT16_T st_gid;
	UINT32_T st_rdev;
	UINT32_T st_size;
	UINT32_T st_blksize;
	UINT32_T st_blocks;
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
	UINT32_T st_p4;
	UINT32_T st_p5;
} __attribute__((packed));

struct pfs_kernel_stat64 {
	UINT64_T st_dev;
	UINT32_T st_pad1;
	UINT32_T st_ino;
	UINT32_T st_mode;
	UINT32_T st_nlink;
	UINT32_T st_uid;
	UINT32_T st_gid;
	UINT64_T st_rdev;
	UINT32_T st_pad2;
	UINT64_T st_size;
	UINT32_T st_blksize;
	UINT64_T st_blocks;
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
	UINT64_T st_ino_extra;
} __attribute__((packed));

struct pfs_kernel_statfs {
        UINT32_T f_type;
        UINT32_T f_bsize;
        UINT32_T f_blocks;
        UINT32_T f_bfree;
        UINT32_T f_bavail;
        UINT32_T f_files;
        UINT32_T f_ffree;
	UINT64_T f_fsid;
        UINT32_T f_namelen;
        UINT32_T f_spare[6];
} __attribute__((packed));

struct pfs_kernel_statfs64 {
        UINT32_T f_type;
        UINT32_T f_bsize;
        UINT64_T f_blocks;
        UINT64_T f_bfree;
        UINT64_T f_bavail;
        UINT64_T f_files;
        UINT64_T f_ffree;
        UINT64_T f_fsid;
        UINT32_T f_namelen;
        UINT32_T f_frsize;
        UINT32_T f_spare[5];
} __attribute__((packed));

struct pfs_kernel_dirent {
        UINT32_T d_ino;
        UINT32_T d_off;
        UINT16_T d_reclen;
        char     d_name[PFS_PATH_MAX];
} __attribute__((packed));

struct pfs_kernel_dirent64 {
        UINT64_T d_ino;
        UINT64_T d_off;
        UINT16_T d_reclen;
	UINT8_T  d_type;
        char     d_name[PFS_PATH_MAX];
} __attribute__((packed));

struct pfs_kernel_iovec {
	UINT32_T iov_base;
	UINT32_T iov_len;
} __attribute__((packed));

struct pfs_kernel_msghdr {
	UINT32_T msg_name;
	UINT32_T msg_namelen;
	UINT32_T msg_iov;
	UINT32_T msg_iovlen;
	UINT32_T msg_control;
	UINT32_T msg_controllen;
	UINT32_T msg_flags;
} __attribute__((packed));

struct pfs_kernel_sigaction {
	UINT32_T pfs_sa_handler;
	UINT32_T pfs_sa_flags;
	UINT32_T pfs_sa_restorer;
	UINT8_T  pfs_sa_mask[128];
} __attribute__((packed));

struct pfs_kernel_ifconf {
	UINT32_T ifc_len;
	UINT32_T ifc_buffer;
} __attribute__ ((packed));

struct pfs_kernel_flock {
	UINT16_T l_type;
	UINT16_T l_whence;
	UINT32_T l_start;
	UINT32_T l_len;
	UINT32_T l_pid;
}__attribute__ ((packed));

struct pfs_kernel_flock64 {
	UINT16_T l_type;
	UINT16_T l_whence;
	UINT64_T l_start;
	UINT64_T l_len;
	UINT32_T l_pid;
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

/*
Many data structures must be aligned on 8 byte boundaries.
This rounds up values to multiples of 8.
*/

#ifndef _ROUND_UP
#define _ROUND_UP(x,n) (((x)+(n)-1u) & ~((n)-1u))
#endif

#ifndef ROUND_UP
#define ROUND_UP(x) _ROUND_UP(x,8LL)
#endif

/*
The size of a dirent is the size of the structure
without the name field, plus the actual length
of the null-terminated name, rounded up to 8-byte alignment.
*/

#define DIRENT_SIZE( x ) \
	ROUND_UP(((char*)&(x).d_name[0]-(char*)&(x)) + strlen((x).d_name) + 1)

#define COPY_DIRENT( a, b ) \
	memset(&(b),0,sizeof((b))); \
	strcpy((b).d_name,(a).d_name);\
	(b).d_ino = (a).d_ino;\
	(b).d_off = (a).d_off;\
	(b).d_reclen = DIRENT_SIZE(b);

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
