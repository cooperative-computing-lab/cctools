/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_GLOBAL_H
#define CHIRP_GLOBAL_H

#include "chirp_client.h"

#include <sys/types.h>
#include <stdio.h>

struct chirp_file *chirp_global_open(const char *host, const char *path, INT64_T flags, INT64_T mode, time_t stoptime);
INT64_T chirp_global_close(struct chirp_file *file, time_t stoptime);
INT64_T chirp_global_pread(struct chirp_file *file, void *buffer, INT64_T length, INT64_T offset, time_t stoptime);
INT64_T chirp_global_pwrite(struct chirp_file *file, const void *buffer, INT64_T length, INT64_T offset, time_t stoptime);
INT64_T chirp_global_sread(struct chirp_file *file, void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime);
INT64_T chirp_global_swrite(struct chirp_file *file, const void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime);
INT64_T chirp_global_fstat(struct chirp_file *file, struct chirp_stat *buf, time_t stoptime);
INT64_T chirp_global_fstatfs(struct chirp_file *file, struct chirp_statfs *buf, time_t stoptime);
INT64_T chirp_global_fchown(struct chirp_file *file, INT64_T uid, INT64_T gid, time_t stoptime);
INT64_T chirp_global_fchmod(struct chirp_file *file, INT64_T mode, time_t stoptime);
INT64_T chirp_global_ftruncate(struct chirp_file *file, INT64_T length, time_t stoptime);
INT64_T chirp_global_flush(struct chirp_file *file, time_t stoptime);

INT64_T chirp_global_getfile(const char *host, const char *path, FILE * stream, time_t stoptime);
INT64_T chirp_global_getfile_buffer(const char *host, const char *path, char **buffer, time_t stoptime);
INT64_T chirp_global_putfile(const char *host, const char *path, FILE * stream, INT64_T mode, INT64_T length, time_t stoptime);
INT64_T chirp_global_putfile_buffer(const char *host, const char *path, const char *buffer, INT64_T mode, INT64_T length, time_t stoptime);
INT64_T chirp_global_whoami(const char *host, const char *path, char *buf, INT64_T length, time_t stoptime);
INT64_T chirp_global_getlongdir(const char *host, const char *path, chirp_longdir_t callback, void *arg, time_t stoptime);
INT64_T chirp_global_getdir(const char *host, const char *path, chirp_dir_t callback, void *arg, time_t stoptime);
INT64_T chirp_global_getacl(const char *host, const char *path, chirp_dir_t callback, void *arg, time_t stoptime);
INT64_T chirp_global_setacl(const char *host, const char *path, const char *subject, const char *rights, time_t stoptime);
INT64_T chirp_global_locate(const char *host, const char *path, chirp_loc_t callback, void *arg, time_t stoptime);
INT64_T chirp_global_unlink(const char *host, const char *path, time_t stoptime);
INT64_T chirp_global_rename(const char *host, const char *path, const char *newpath, time_t stoptime);
INT64_T chirp_global_link(const char *host, const char *path, const char *newpath, time_t stoptime);
INT64_T chirp_global_symlink(const char *host, const char *path, const char *newpath, time_t stoptime);
INT64_T chirp_global_readlink(const char *host, const char *path, char *buf, INT64_T length, time_t stoptime);
INT64_T chirp_global_mkdir(const char *host, const char *path, INT64_T mode, time_t stoptime);
INT64_T chirp_global_rmdir(const char *host, const char *path, time_t stoptime);
INT64_T chirp_global_rmall(const char *host, const char *path, time_t stoptime);
INT64_T chirp_global_stat(const char *host, const char *path, struct chirp_stat *buf, time_t stoptime);
INT64_T chirp_global_lstat(const char *host, const char *path, struct chirp_stat *buf, time_t stoptime);
INT64_T chirp_global_statfs(const char *host, const char *path, struct chirp_statfs *buf, time_t stoptime);
INT64_T chirp_global_access(const char *host, const char *path, INT64_T mode, time_t stoptime);
INT64_T chirp_global_chmod(const char *host, const char *path, INT64_T mode, time_t stoptime);
INT64_T chirp_global_chown(const char *host, const char *path, INT64_T uid, INT64_T gid, time_t stoptime);
INT64_T chirp_global_lchown(const char *host, const char *path, INT64_T uid, INT64_T gid, time_t stoptime);
INT64_T chirp_global_truncate(const char *host, const char *path, INT64_T length, time_t stoptime);
INT64_T chirp_global_utime(const char *host, const char *path, time_t actime, time_t modtime, time_t stoptime);
INT64_T chirp_global_thirdput(const char *host, const char *path, const char *thirdhost, const char *thirdpath, time_t stoptime);
INT64_T chirp_global_md5(const char *host, const char *path, unsigned char *digest, time_t stoptime);
INT64_T chirp_global_mkalloc(const char *host, const char *path, INT64_T size, INT64_T mode, time_t stoptime);
INT64_T chirp_global_lsalloc(const char *host, const char *path, char *alloc_path, INT64_T * size, INT64_T * inuse, time_t stoptime);

INT64_T chirp_global_getxattr(const char *host, const char *path, const char *name, void *data, size_t size, time_t stoptime);
INT64_T chirp_global_fgetxattr(struct chirp_file *file, const char *name, void *data, size_t size, time_t stoptime);
INT64_T chirp_global_lgetxattr(const char *host, const char *path, const char *name, void *data, size_t size, time_t stoptime);
INT64_T chirp_global_listxattr(const char *host, const char *path, char *list, size_t size, time_t stoptime);
INT64_T chirp_global_flistxattr(struct chirp_file *file, char *list, size_t size, time_t stoptime);
INT64_T chirp_global_llistxattr(const char *host, const char *path, char *list, size_t size, time_t stoptime);
INT64_T chirp_global_setxattr(const char *host, const char *path, const char *name, const void *data, size_t size, int flags, time_t stoptime);
INT64_T chirp_global_fsetxattr(struct chirp_file *file, const char *name, const void *data, size_t size, int flags, time_t stoptime);
INT64_T chirp_global_lsetxattr(const char *host, const char *path, const char *name, const void *data, size_t size, int flags, time_t stoptime);
INT64_T chirp_global_removexattr(const char *host, const char *path, const char *name, time_t stoptime);
INT64_T chirp_global_fremovexattr(struct chirp_file *file, const char *name, time_t stoptime);
INT64_T chirp_global_lremovexattr(const char *host, const char *path, const char *name, time_t stoptime);

void chirp_global_inhibit_catalog(int onoff);

#endif

/* vim: set noexpandtab tabstop=8: */
