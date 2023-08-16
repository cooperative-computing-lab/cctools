/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_CLIENT_H
#define CHIRP_CLIENT_H

#include "int_sizes.h"

#include "chirp_protocol.h"
#include "chirp_types.h"

#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <time.h>

/* Authentication Environment Variable */
#define CHIRP_CLIENT_TICKETS  "CHIRP_CLIENT_TICKETS"

struct chirp_client *chirp_client_connect(const char *host, int negotiate_auth, time_t stoptime);
struct chirp_client *chirp_client_connect_condor(time_t stoptime);

void chirp_client_disconnect(struct chirp_client *c);
INT64_T chirp_client_serial(struct chirp_client *c);

INT64_T chirp_client_open(struct chirp_client *c, const char *path, INT64_T flags, INT64_T mode, struct chirp_stat *buf, time_t stoptime);
INT64_T chirp_client_close(struct chirp_client *c, INT64_T fd, time_t stoptime);
INT64_T chirp_client_pread(struct chirp_client *c, INT64_T fd, void *buffer, INT64_T length, INT64_T offset, time_t stoptime);
INT64_T chirp_client_pwrite(struct chirp_client *c, INT64_T fd, const void *buffer, INT64_T length, INT64_T offset, time_t stoptime);
INT64_T chirp_client_sread(struct chirp_client *c, INT64_T fd, void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime);
INT64_T chirp_client_swrite(struct chirp_client *c, INT64_T fd, const void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime);
INT64_T chirp_client_fsync(struct chirp_client *c, INT64_T fd, time_t stoptime);
INT64_T chirp_client_fstat(struct chirp_client *c, INT64_T fd, struct chirp_stat *buf, time_t stoptime);
INT64_T chirp_client_fstatfs(struct chirp_client *c, INT64_T fd, struct chirp_statfs *buf, time_t stoptime);
INT64_T chirp_client_fchown(struct chirp_client *c, INT64_T fd, INT64_T uid, INT64_T gid, time_t stoptime);
INT64_T chirp_client_fchmod(struct chirp_client *c, INT64_T fd, INT64_T mode, time_t stoptime);
INT64_T chirp_client_ftruncate(struct chirp_client *c, INT64_T fd, INT64_T length, time_t stoptime);

INT64_T chirp_client_getfile(struct chirp_client *c, const char *name, FILE * stream, time_t stoptime);
INT64_T chirp_client_getfile_buffer(struct chirp_client *c, const char *name, char **buffer, time_t stoptime);
INT64_T chirp_client_putfile(struct chirp_client *c, const char *name, FILE * stream, INT64_T mode, INT64_T length, time_t stoptime);
INT64_T chirp_client_putfile_buffer(struct chirp_client *c, const char *name, const void *buffer, INT64_T mode, size_t length, time_t stoptime);
INT64_T chirp_client_thirdput(struct chirp_client *c, const char *path, const char *hostname, const char *newpath, time_t stoptime);

INT64_T chirp_client_getstream(struct chirp_client *c, const char *path, time_t stoptime);
INT64_T chirp_client_getstream_read(struct chirp_client *c, void *buffer, INT64_T length, time_t stoptime);

INT64_T chirp_client_putstream(struct chirp_client *c, const char *path, time_t stoptime);
INT64_T chirp_client_putstream_write(struct chirp_client *c, const char *data, INT64_T length, time_t stoptime);

INT64_T chirp_client_cookie(struct chirp_client *c, const char *cookie, time_t stoptime);

CHIRP_SEARCH *chirp_client_opensearch(struct chirp_client *c, const char *paths, const char *pattern, int flags, time_t stoptime);
struct chirp_searchent *chirp_client_readsearch(CHIRP_SEARCH *search);
int chirp_client_closesearch(CHIRP_SEARCH *search);

INT64_T chirp_client_getlongdir(struct chirp_client *c, const char *path, chirp_longdir_t callback, void *arg, time_t stoptime);
INT64_T chirp_client_getdir(struct chirp_client *c, const char *path, chirp_dir_t callback, void *arg, time_t stoptime);
INT64_T chirp_client_opendir(struct chirp_client *c, const char *path, time_t stoptime);
const char *chirp_client_readdir(struct chirp_client *c, time_t stoptime);
INT64_T chirp_client_getacl(struct chirp_client *c, const char *path, chirp_dir_t callback, void *arg, time_t stoptime);
INT64_T chirp_client_openacl(struct chirp_client *c, const char *path, time_t stoptime);
const char *chirp_client_readacl(struct chirp_client *c, time_t stoptime);
INT64_T chirp_client_ticket_create(struct chirp_client *c, char name[CHIRP_PATH_MAX], unsigned bits, time_t stoptime);
INT64_T chirp_client_ticket_register(struct chirp_client *c, const char *name, const char *subject, time_t duration, time_t stoptime);
INT64_T chirp_client_ticket_delete(struct chirp_client *c, const char *name, time_t stoptime);
INT64_T chirp_client_ticket_list(struct chirp_client *c, const char *subject, char ***list, time_t stoptime);
INT64_T chirp_client_ticket_get(struct chirp_client *c, const char *name, char **subject, char **ticket, time_t * duration, char ***rights, time_t stoptime);
INT64_T chirp_client_ticket_modify(struct chirp_client *c, const char *name, const char *path, const char *aclmask, time_t stoptime);
INT64_T chirp_client_setacl(struct chirp_client *c, const char *path, const char *user, const char *acl, time_t stoptime);
INT64_T chirp_client_resetacl(struct chirp_client *c, const char *path, const char *acl, time_t stoptime);
INT64_T chirp_client_locate(struct chirp_client *c, const char *path, chirp_loc_t callback, void *arg, time_t stoptime);
INT64_T chirp_client_whoami(struct chirp_client *c, char *buf, INT64_T length, time_t stoptime);
INT64_T chirp_client_whoareyou(struct chirp_client *c, const char *rhost, char *buffer, INT64_T length, time_t stoptime);
INT64_T chirp_client_unlink(struct chirp_client *c, const char *path, time_t stoptime);
INT64_T chirp_client_rename(struct chirp_client *c, const char *path, const char *newpath, time_t stoptime);
INT64_T chirp_client_link(struct chirp_client *c, const char *path, const char *newpath, time_t stoptime);
INT64_T chirp_client_symlink(struct chirp_client *c, const char *path, const char *newpath, time_t stoptime);
INT64_T chirp_client_readlink(struct chirp_client *c, const char *path, char *buf, INT64_T length, time_t stoptime);
INT64_T chirp_client_mkdir(struct chirp_client *c, char const *path, INT64_T mode, time_t stoptime);
INT64_T chirp_client_rmdir(struct chirp_client *c, char const *path, time_t stoptime);
INT64_T chirp_client_rmall(struct chirp_client *c, char const *path, time_t stoptime);
INT64_T chirp_client_stat(struct chirp_client *c, const char *path, struct chirp_stat *buf, time_t stoptime);
INT64_T chirp_client_lstat(struct chirp_client *c, const char *path, struct chirp_stat *buf, time_t stoptime);
INT64_T chirp_client_statfs(struct chirp_client *c, const char *path, struct chirp_statfs *buf, time_t stoptime);
INT64_T chirp_client_access(struct chirp_client *c, const char *path, INT64_T mode, time_t stoptime);
INT64_T chirp_client_chmod(struct chirp_client *c, const char *path, INT64_T mode, time_t stoptime);
INT64_T chirp_client_chown(struct chirp_client *c, const char *path, INT64_T uid, INT64_T gid, time_t stoptime);
INT64_T chirp_client_lchown(struct chirp_client *c, const char *path, INT64_T uid, INT64_T gid, time_t stoptime);
INT64_T chirp_client_truncate(struct chirp_client *c, const char *path, INT64_T length, time_t stoptime);
INT64_T chirp_client_utime(struct chirp_client *c, const char *path, time_t actime, time_t modtime, time_t stoptime);
INT64_T chirp_client_hash(struct chirp_client *c, const char *path, const char *algorithm, unsigned char digest[CHIRP_DIGEST_MAX], time_t stoptime);
INT64_T chirp_client_md5(struct chirp_client *c, const char *path, unsigned char digest[CHIRP_DIGEST_MAX], time_t stoptime);
INT64_T chirp_client_setrep(struct chirp_client *c, const char *path, int nreps, time_t stoptime);

INT64_T chirp_client_getxattr(struct chirp_client *c, const char *path, const char *name, void *data, size_t size, time_t stoptime);
INT64_T chirp_client_fgetxattr(struct chirp_client *c, INT64_T fd, const char *name, void *data, size_t size, time_t stoptime);
INT64_T chirp_client_lgetxattr(struct chirp_client *c, const char *path, const char *name, void *data, size_t size, time_t stoptime);
INT64_T chirp_client_listxattr(struct chirp_client *c, const char *path, char *list, size_t size, time_t stoptime);
INT64_T chirp_client_flistxattr(struct chirp_client *c, INT64_T fd, char *list, size_t size, time_t stoptime);
INT64_T chirp_client_llistxattr(struct chirp_client *c, const char *path, char *list, size_t size, time_t stoptime);
INT64_T chirp_client_setxattr(struct chirp_client *c, const char *path, const char *name, const void *data, size_t size, int flags, time_t stoptime);
INT64_T chirp_client_fsetxattr(struct chirp_client *c, INT64_T fd, const char *name, const void *data, size_t size, int flags, time_t stoptime);
INT64_T chirp_client_lsetxattr(struct chirp_client *c, const char *path, const char *name, const void *data, size_t size, int flags, time_t stoptime);
INT64_T chirp_client_removexattr(struct chirp_client *c, const char *path, const char *name, time_t stoptime);
INT64_T chirp_client_fremovexattr(struct chirp_client *c, INT64_T fd, const char *name, time_t stoptime);
INT64_T chirp_client_lremovexattr(struct chirp_client *c, const char *path, const char *name, time_t stoptime);

INT64_T chirp_client_remote_debug(struct chirp_client *c, const char *flag, time_t stoptime);
INT64_T chirp_client_localpath(struct chirp_client *c, const char *path, char *localpath, int length, time_t stoptime);
INT64_T chirp_client_audit(struct chirp_client *c, const char *path, struct chirp_audit **list, time_t stoptime);

INT64_T chirp_client_mkalloc(struct chirp_client *c, char const *path, INT64_T size, INT64_T mode, time_t stoptime);
INT64_T chirp_client_lsalloc(struct chirp_client *c, char const *path, char *allocpath, INT64_T * total, INT64_T * inuse, time_t stoptime);

INT64_T chirp_client_pread_begin(struct chirp_client *c, INT64_T fd, void *buffer, INT64_T length, INT64_T offset, time_t stoptime);
INT64_T chirp_client_pread_finish(struct chirp_client *c, INT64_T fd, void *buffer, INT64_T length, INT64_T offset, time_t stoptime);
INT64_T chirp_client_sread_begin(struct chirp_client *c, INT64_T fd, void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime);
INT64_T chirp_client_sread_finish(struct chirp_client *c, INT64_T fd, void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime);
INT64_T chirp_client_pwrite_begin(struct chirp_client *c, INT64_T fd, const void *buffer, INT64_T length, INT64_T offset, time_t stoptime);
INT64_T chirp_client_pwrite_finish(struct chirp_client *c, INT64_T fd, const void *buffer, INT64_T length, INT64_T offset, time_t stoptime);
INT64_T chirp_client_swrite_begin(struct chirp_client *c, INT64_T fd, const void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime);
INT64_T chirp_client_swrite_finish(struct chirp_client *c, INT64_T fd, const void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime);
INT64_T chirp_client_fsync_begin(struct chirp_client *c, INT64_T fd, time_t stoptime);
INT64_T chirp_client_fsync_finish(struct chirp_client *c, INT64_T fd, time_t stoptime);
INT64_T chirp_client_fstat_begin(struct chirp_client *c, INT64_T fd, struct chirp_stat *buf, time_t stoptime);
INT64_T chirp_client_fstat_finish(struct chirp_client *c, INT64_T fd, struct chirp_stat *buf, time_t stoptime);

INT64_T chirp_client_job_create(struct chirp_client *c, const char *json, chirp_jobid_t *id, time_t stoptime);
INT64_T chirp_client_job_commit(struct chirp_client *c, const char *json, time_t stoptime);
INT64_T chirp_client_job_kill(struct chirp_client *c, const char *json, time_t stoptime);
INT64_T chirp_client_job_status(struct chirp_client *c, const char *json, char **status, time_t stoptime);
INT64_T chirp_client_job_wait(struct chirp_client *c, chirp_jobid_t id, INT64_T timeout, char **status, time_t stoptime);
INT64_T chirp_client_job_reap(struct chirp_client *c, const char *json, time_t stoptime);

#endif

/* vim: set noexpandtab tabstop=8: */
