/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_ALLOC_H
#define CHIRP_ALLOC_H

#include "chirp_types.h"
#include "link.h"

#include <sys/types.h>
#include <stdio.h>

void chirp_alloc_init(const char *rootpath, INT64_T size);
void chirp_alloc_flush();
int chirp_alloc_flush_needed();
time_t chirp_alloc_last_flush_time();

INT64_T chirp_alloc_open(const char *path, INT64_T flags, INT64_T mode);
INT64_T chirp_alloc_close(int fd);
INT64_T chirp_alloc_pread(int fd, void *buffer, INT64_T length, INT64_T offset);
INT64_T chirp_alloc_pwrite(int fd, const void *buffer, INT64_T length, INT64_T offset);
INT64_T chirp_alloc_sread(int fd, void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset);
INT64_T chirp_alloc_swrite(int fd, const void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset);
INT64_T chirp_alloc_fstat(int fd, struct chirp_stat *buf);
INT64_T chirp_alloc_fstatfs(int fd, struct chirp_statfs *buf);
INT64_T chirp_alloc_fchown(int fd, INT64_T uid, INT64_T gid);
INT64_T chirp_alloc_fchmod(int fd, INT64_T mode);
INT64_T chirp_alloc_ftruncate(int fd, INT64_T length);
INT64_T chirp_alloc_fsync(int fd);

void *chirp_alloc_opendir(const char *path);
char *chirp_alloc_readdir(void *dir);
void chirp_alloc_closedir(void *dir);

INT64_T chirp_alloc_getfile(const char *path, struct link *link, time_t stoptime);
INT64_T chirp_alloc_putfile(const char *path, struct link *link, INT64_T length, INT64_T mode, time_t stoptime);

INT64_T chirp_alloc_getstream(const char *path, struct link *link, time_t stoptime);
INT64_T chirp_alloc_putstream(const char *path, struct link *link, time_t stoptime);

INT64_T chirp_alloc_mkfifo(const char *path);
INT64_T chirp_alloc_unlink(const char *path);
INT64_T chirp_alloc_rename(const char *path, const char *newpath);
INT64_T chirp_alloc_link(const char *path, const char *newpath);
INT64_T chirp_alloc_symlink(const char *path, const char *newpath);
INT64_T chirp_alloc_readlink(const char *path, char *buf, INT64_T length);
INT64_T chirp_alloc_mkdir(const char *path, INT64_T mode);
INT64_T chirp_alloc_rmdir(const char *path);
INT64_T chirp_alloc_rmall(const char *path);
INT64_T chirp_alloc_stat(const char *path, struct chirp_stat *buf);
INT64_T chirp_alloc_lstat(const char *path, struct chirp_stat *buf);
INT64_T chirp_alloc_statfs(const char *path, struct chirp_statfs *buf);
INT64_T chirp_alloc_access(const char *path, INT64_T mode);
INT64_T chirp_alloc_chmod(const char *path, INT64_T mode);
INT64_T chirp_alloc_chown(const char *path, INT64_T uid, INT64_T gid);
INT64_T chirp_alloc_lchown(const char *path, INT64_T uid, INT64_T gid);
INT64_T chirp_alloc_truncate(const char *path, INT64_T length);
INT64_T chirp_alloc_utime(const char *path, time_t actime, time_t modtime);
INT64_T chirp_alloc_md5(const char *path, unsigned char digest[16]);

INT64_T chirp_alloc_lsalloc(const char *path, char *alloc_path, INT64_T * total, INT64_T * inuse);
INT64_T chirp_alloc_mkalloc(const char *path, INT64_T size, INT64_T mode);

INT64_T chirp_alloc_file_size(const char *path);
INT64_T chirp_alloc_fd_size(int fd);

#endif
