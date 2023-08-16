/*
Copyright (C) 2004 Douglas Thain
This work is made available under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_MULTI_H
#define CHIRP_MULTI_H

#include "chirp_client.h"

#include <sys/types.h>
#include <stdio.h>

struct chirp_file *chirp_multi_open(const char *volume, const char *path, INT64_T flags, INT64_T mode, time_t stoptime);
INT64_T chirp_multi_close(struct chirp_file *file, time_t stoptime);
INT64_T chirp_multi_pread(struct chirp_file *file, void *buffer, INT64_T length, INT64_T offset, time_t stoptime);
INT64_T chirp_multi_pwrite(struct chirp_file *file, const void *buffer, INT64_T length, INT64_T offset, time_t stoptime);
INT64_T chirp_multi_fstat(struct chirp_file *file, struct chirp_stat *buf, time_t stoptime);
INT64_T chirp_multi_fstatfs(struct chirp_file *file, struct chirp_statfs *buf, time_t stoptime);
INT64_T chirp_multi_fchown(struct chirp_file *file, INT64_T uid, INT64_T gid, time_t stoptime);
INT64_T chirp_multi_fchmod(struct chirp_file *file, INT64_T mode, time_t stoptime);
INT64_T chirp_multi_ftruncate(struct chirp_file *file, INT64_T length, time_t stoptime);
INT64_T chirp_multi_flush(struct chirp_file *file, time_t stoptime);

INT64_T chirp_multi_getfile(const char *volume, const char *path, FILE * stream, time_t stoptime);
INT64_T chirp_multi_getfile_buffer(const char *volume, const char *path, char **buffer, time_t stoptime);
INT64_T chirp_multi_putfile(const char *volume, const char *path, FILE * stream, INT64_T mode, INT64_T length, time_t stoptime);
INT64_T chirp_multi_putfile_buffer(const char *volume, const char *path, const char *buffer, INT64_T mode, INT64_T length, time_t stoptime);

INT64_T chirp_multi_getlongdir(const char *volume, const char *path, chirp_longdir_t callback, void *arg, time_t stoptime);
INT64_T chirp_multi_getdir(const char *volume, const char *path, chirp_dir_t callback, void *arg, time_t stoptime);
INT64_T chirp_multi_getacl(const char *volume, const char *path, chirp_dir_t callback, void *arg, time_t stoptime);
INT64_T chirp_multi_setacl(const char *volume, const char *path, const char *subject, const char *rights, time_t stoptime);
INT64_T chirp_multi_locate(const char *volume, const char *path, chirp_loc_t callback, void *arg, time_t stoptime);
INT64_T chirp_multi_whoami(const char *volume, char *buf, INT64_T length, time_t stoptime);
INT64_T chirp_multi_unlink(const char *volume, const char *path, time_t stoptime);
INT64_T chirp_multi_rename(const char *volume, const char *path, const char *newpath, time_t stoptime);
INT64_T chirp_multi_link(const char *volume, const char *path, const char *newpath, time_t stoptime);
INT64_T chirp_multi_symlink(const char *volume, const char *path, const char *newpath, time_t stoptime);
INT64_T chirp_multi_readlink(const char *volume, const char *path, char *buf, INT64_T length, time_t stoptime);
INT64_T chirp_multi_mkdir(const char *volume, char const *path, INT64_T mode, time_t stoptime);
INT64_T chirp_multi_rmdir(const char *volume, char const *path, time_t stoptime);
INT64_T chirp_multi_stat(const char *volume, const char *path, struct chirp_stat *buf, time_t stoptime);
INT64_T chirp_multi_lstat(const char *volume, const char *path, struct chirp_stat *buf, time_t stoptime);
INT64_T chirp_multi_statfs(const char *volume, const char *path, struct chirp_statfs *buf, time_t stoptime);
INT64_T chirp_multi_access(const char *volume, const char *path, INT64_T mode, time_t stoptime);
INT64_T chirp_multi_chmod(const char *volume, const char *path, INT64_T mode, time_t stoptime);
INT64_T chirp_multi_chown(const char *volume, const char *path, INT64_T uid, INT64_T gid, time_t stoptime);
INT64_T chirp_multi_lchown(const char *volume, const char *path, INT64_T uid, INT64_T gid, time_t stoptime);
INT64_T chirp_multi_truncate(const char *volume, const char *path, INT64_T length, time_t stoptime);
INT64_T chirp_multi_utime(const char *volume, const char *path, time_t actime, time_t modtime, time_t stoptime);
INT64_T chirp_multi_md5(const char *volume, const char *path, unsigned char digest[CHIRP_DIGEST_MAX], time_t stoptime);

#endif

/* vim: set noexpandtab tabstop=8: */
