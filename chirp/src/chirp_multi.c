/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_multi.h"
#include "chirp_reli.h"
#include "chirp_protocol.h"

#include "debug.h"
#include "stringtools.h"
#include "create_dir.h"
#include "hash_table.h"
#include "xxmalloc.h"
#include "md5.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#ifdef HAS_SYS_STATFS_H
#include <sys/statfs.h>
#endif
#include <sys/param.h>
#include <sys/mount.h>
#include <dirent.h>
#include <utime.h>
#include <pwd.h>
#include <ctype.h>

struct chirp_file {
	char host[CHIRP_LINE_MAX];
	char path[CHIRP_LINE_MAX];
	struct chirp_stat info;
	INT64_T fd;
	INT64_T flags;
	INT64_T mode;
	INT64_T serial;
	INT64_T stale;
};

struct chirp_volume *current_volume = 0;

struct file_info {
	char lpath[CHIRP_PATH_MAX];
	char rpath[CHIRP_PATH_MAX];
	char rhost[CHIRP_PATH_MAX];
};

struct chirp_server {
	char name[CHIRP_PATH_MAX];
	int priority;
	int prepared;
};

struct chirp_volume {
	char name[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	char root[CHIRP_PATH_MAX];
	char key[CHIRP_PATH_MAX];
	struct chirp_server **servers;
	int nservers;
};

struct chirp_volume *chirp_volume_open(const char *volume, time_t stoptime)
{
	struct chirp_volume *v;
	char filename[CHIRP_PATH_MAX];
	char *buffer;
	char *name;
	char *c;
	int result;

	debug(D_MULTI, "opening volume %s", volume);
	/*
	   The volume name must have at least one
	   at-sign in order to indicate the volname.
	 */

	c = strchr(volume, '@');
	if(!c) {
		errno = ENOENT;
		return 0;
	}

	v = xxmalloc(sizeof(*v));
	strcpy(v->name, volume);

	/*
	   The stuff preceding the first at-sign is
	   the name of the host containing the directory.
	 */

	strcpy(v->host, v->name);
	c = strchr(v->host, '@');

	/* The rest is the logical name of the volume. */
	strcpy(v->root, c);
	*c = 0;

	/* Convert any remaning at-signs to slashes */
	for(c = v->root; *c; c++) {
		if(*c == '@')
			*c = '/';
	}

	/* Fetch the filesystem key */
	string_nformat(filename, sizeof(filename), "%s/key", v->root);
	result = chirp_reli_getfile_buffer(v->host, filename, &buffer, stoptime);
	if(result < 0) {
		debug(D_CHIRP, "couldn't open %s: %s", filename, strerror(errno));
		free(v);
		errno = ENOENT;
		return 0;
	}
	strcpy(v->key, buffer);
	string_chomp(v->key);
	free(buffer);

	/* Fetch the list of hosts */
	string_nformat(filename, sizeof(filename), "%s/hosts", v->root);
	result = chirp_reli_getfile_buffer(v->host, filename, &buffer, stoptime);
	if(result < 0) {
		debug(D_CHIRP, "couldn't open %s: %s", filename, strerror(errno));
		free(v);
		errno = ENOENT;
		return 0;
	}

	/* Get an upper bound on the servers by counting the whitespace */
	int maxservers = 0;
	for(c = buffer; *c; c++)
		if(isspace((int) *c))
			maxservers++;

	/* Allocate space for an array of servers */
	v->servers = malloc(sizeof(struct chirp_server *) * maxservers);
	v->nservers = 0;
	debug(D_MULTI, "estimating %d servers", maxservers);

	/* Break into whitespace and initialize the servers. */

	name = strtok(buffer, " \t\n");
	while(name) {
		debug(D_MULTI, "server: %s", name);
		struct chirp_server *s = xxmalloc(sizeof(*s));
		strcpy(s->name, name);
		s->priority = 0;
		s->prepared = 0;
		v->servers[v->nservers++] = s;
		name = strtok(0, " \t\n");
	}

	/* randomly initialize the priority of the servers */
	int i;
	int n = rand() % v->nservers;
	for(i = 0; i < n; i++)
		v->servers[i]->priority++;

	free(buffer);

	return v;
}

void chirp_volume_close(struct chirp_volume *v)
{
	int i;
	for(i = 0; i < v->nservers; i++)
		free(v->servers[i]);
	free(v->servers);
	free(v);
}

struct chirp_server *chirp_volume_server_choose(struct chirp_volume *v)
{
	struct chirp_server *best_server = 0;
	struct chirp_server *s;
	int i;

	for(i = 0; i < v->nservers; i++) {
		s = v->servers[i];
		if(!best_server || s->priority < best_server->priority) {
			best_server = s;
		}
	}

	return best_server;
}

static int chirp_multi_init(const char *volume, time_t stoptime)
{
	int cpos, apos;
	char *c;

	cpos = strrpos(volume, ':');
	apos = strrpos(volume, '@');
	if(cpos > apos) {
		c = strrchr(volume, ':');
		if(c)
			*c = 0;
	}

	debug(D_MULTI, "init: /multi/%s", volume);

	if(current_volume && strcmp(current_volume->name, volume)) {
		chirp_volume_close(current_volume);
		current_volume = 0;
	}

	if(!current_volume) {
		current_volume = chirp_volume_open(volume, stoptime);
		if(!current_volume)
			return 0;
	}

	return 1;
}

static int chirp_multi_lpath(const char *volume, const char *path, char *lpath, time_t stoptime)
{
	/*
	   Hack: df tries to search above /multi/volume/..
	   and then gets confused.  Stop it at the root.
	 */
	if(!strncmp(path, "/..", 3))
		path = "/";

	if(!chirp_multi_init(volume, stoptime))
		return 0;
	string_nformat(lpath, CHIRP_PATH_MAX, "%s/root/%s", current_volume->root, path);
	return 1;
}

static int chirp_multi_lookup(const char *volume, const char *path, struct file_info *info, time_t stoptime)
{
	int result, fields;
	char *buffer;

	if(!chirp_multi_lpath(volume, path, info->lpath, stoptime))
		return 0;

	result = chirp_reli_getfile_buffer(current_volume->host, info->lpath, &buffer, stoptime);
	if(result <= 0)
		return 0;

	fields = sscanf(buffer, "%s %s", info->rhost, info->rpath);

	free(buffer);

	debug(D_MULTI, "lookup: /multi/%s%s at /chirp/%s/%s", volume, path, info->rhost, info->rpath);

	if(fields == 2) {
		return 1;
	} else {
		errno = EIO;
		return 0;
	}
}

static int chirp_multi_update(const char *volume, const char *path, struct file_info *info, time_t stoptime)
{
	char buffer[CHIRP_PATH_MAX * 2 + 2];
	if(!chirp_multi_lpath(volume, path, info->lpath, stoptime))
		return 0;
	string_nformat(buffer, sizeof(buffer), "%s\n%s\n", info->rhost, info->rpath);
	return chirp_reli_putfile_buffer(current_volume->host, info->lpath, buffer, 0700, strlen(buffer), stoptime);
}

static struct chirp_file *chirp_multi_create(const char *volume, const char *path, INT64_T flags, INT64_T mode, time_t stoptime)
{
	struct chirp_server *server;
	struct file_info info;

	if(!chirp_multi_lpath(volume, path, info.lpath, stoptime))
		return 0;

	while(1) {
		server = chirp_volume_server_choose(current_volume);
		if(!server) {
			errno = ENOSPC;
			return 0;
		}

		if(!server->prepared) {
			debug(D_MULTI, "preparing server %s", server->name);
			char keypath[CHIRP_PATH_MAX];
			string_nformat(keypath, sizeof(keypath), "/%s", current_volume->key);
			int result = chirp_reli_mkdir_recursive(server->name, keypath, 0777, stoptime);
			if(result < 0 && errno != EEXIST) {
				server->priority += 10;
				continue;
			}
			server->prepared = 1;
		}

		char cookie[17];
		strcpy(info.rhost, server->name);
		string_cookie(cookie, 16);
		string_nformat(info.rpath, sizeof(info.rpath), "%s/%s", current_volume->key, cookie);

		debug(D_MULTI, "create: /multi/%s%s at /chirp/%s/%s", volume, path, info.rhost, info.rpath);
		if(chirp_multi_update(volume, path, &info, stoptime) < 0)
			return 0;

		/* O_EXCL is needed to make sure that we don't accidentally choose the same local name twice. */
		struct chirp_file *file = chirp_reli_open(info.rhost, info.rpath, flags | O_EXCL, mode, stoptime);
		if(file) {
			server->priority += 1;
			return file;
		} else {
			debug(D_MULTI, "create failed, trying another server...");
			server->priority += 10;
		}
	}


}

struct chirp_file *chirp_multi_open(const char *volume, const char *path, INT64_T flags, INT64_T mode, time_t stoptime)
{
	struct file_info info;

	if(!chirp_multi_lookup(volume, path, &info, stoptime)) {
		if(errno == ENOENT && flags & O_CREAT) {
			return chirp_multi_create(volume, path, flags, mode, stoptime);
		} else {
			return 0;
		}
	}

	return chirp_reli_open(info.rhost, info.rpath, flags, mode, stoptime);
}

INT64_T chirp_multi_close(struct chirp_file * file, time_t stoptime)
{
	return chirp_reli_close(file, stoptime);
}

INT64_T chirp_multi_pread(struct chirp_file * file, void *buffer, INT64_T length, INT64_T offset, time_t stoptime)
{
	return chirp_reli_pread(file, buffer, length, offset, stoptime);
}

INT64_T chirp_multi_pwrite(struct chirp_file * file, const void *buffer, INT64_T length, INT64_T offset, time_t stoptime)
{
	return chirp_reli_pwrite(file, buffer, length, offset, stoptime);
}

INT64_T chirp_multi_fstat(struct chirp_file * file, struct chirp_stat * buf, time_t stoptime)
{
	return chirp_reli_fstat(file, buf, stoptime);
}

INT64_T chirp_multi_fstatfs(struct chirp_file * file, struct chirp_statfs * buf, time_t stoptime)
{
	return chirp_reli_fstatfs(file, buf, stoptime);
}

INT64_T chirp_multi_fchown(struct chirp_file * file, INT64_T uid, INT64_T gid, time_t stoptime)
{
	return chirp_reli_fchown(file, uid, gid, stoptime);
}

INT64_T chirp_multi_fchmod(struct chirp_file * file, INT64_T mode, time_t stoptime)
{
	return chirp_reli_fchmod(file, mode, stoptime);
}

INT64_T chirp_multi_ftruncate(struct chirp_file * file, INT64_T length, time_t stoptime)
{
	return chirp_reli_ftruncate(file, length, stoptime);
}

INT64_T chirp_multi_flush(struct chirp_file * file, time_t stoptime)
{
	return chirp_reli_flush(file, stoptime);
}

INT64_T chirp_multi_getfile(const char *volume, const char *path, FILE * stream, time_t stoptime)
{
	struct file_info info;
	if(!chirp_multi_lookup(volume, path, &info, stoptime))
		return -1;
	return chirp_reli_getfile(info.rhost, info.rpath, stream, stoptime);
}

INT64_T chirp_multi_getfile_buffer(const char *volume, const char *path, char **buffer, time_t stoptime)
{
	struct file_info info;
	if(!chirp_multi_lookup(volume, path, &info, stoptime))
		return -1;
	return chirp_reli_getfile_buffer(info.rhost, info.rpath, buffer, stoptime);
}

INT64_T chirp_multi_putfile(const char *volume, const char *path, FILE * stream, INT64_T mode, INT64_T length, time_t stoptime)
{
	struct file_info info;
	struct chirp_file *file;

	if(!chirp_multi_lookup(volume, path, &info, stoptime)) {
		file = chirp_multi_create(volume, path, O_CREAT | O_TRUNC | O_WRONLY, mode, stoptime);
		if(!file)
			return -1;
		chirp_multi_close(file, stoptime);
		if(!chirp_multi_lookup(volume, path, &info, stoptime))
			return -1;
	}
	return chirp_reli_putfile(info.rhost, info.rpath, stream, mode, length, stoptime);
}

INT64_T chirp_multi_putfile_buffer(const char *volume, const char *path, const char *buffer, INT64_T mode, INT64_T length, time_t stoptime)
{
	struct file_info info;
	struct chirp_file *file;

	if(!chirp_multi_lookup(volume, path, &info, stoptime)) {
		file = chirp_multi_create(volume, path, O_CREAT | O_TRUNC | O_WRONLY, mode, stoptime);
		if(!file)
			return -1;
		chirp_multi_close(file, stoptime);
		if(!chirp_multi_lookup(volume, path, &info, stoptime))
			return -1;
	}
	return chirp_reli_putfile_buffer(info.rhost, info.rpath, buffer, mode, length, stoptime);
}

INT64_T chirp_multi_getdir(const char *volume, const char *path, chirp_dir_t callback, void *arg, time_t stoptime)
{
	char lpath[CHIRP_PATH_MAX];
	if(!volume[0]) {
		callback(".", arg);
		callback("..", arg);
		return 0;
	} else if(!chirp_multi_lpath(volume, path, lpath, stoptime)) {
		return -1;
	} else {
		return chirp_reli_getdir(current_volume->host, lpath, callback, arg, stoptime);
	}
}

INT64_T chirp_multi_getlongdir(const char *volume, const char *path, chirp_longdir_t callback, void *arg, time_t stoptime)
{
	char lpath[CHIRP_PATH_MAX];
	if(!volume[0]) {
		/* FIXME callbacks with "." and ".."? */
		return 0;
	} else if(!chirp_multi_lpath(volume, path, lpath, stoptime)) {
		return -1;
	} else {
		return chirp_reli_getlongdir(current_volume->host, lpath, callback, arg, stoptime);
	}
}

INT64_T chirp_multi_getacl(const char *volume, const char *path, chirp_dir_t callback, void *arg, time_t stoptime)
{
	char lpath[CHIRP_PATH_MAX];
	if(!chirp_multi_lpath(volume, path, lpath, stoptime))
		return -1;
	return chirp_reli_getacl(current_volume->host, lpath, callback, arg, stoptime);
}

INT64_T chirp_multi_setacl(const char *volume, const char *path, const char *subject, const char *rights, time_t stoptime)
{
	char lpath[CHIRP_PATH_MAX];
	if(!chirp_multi_lpath(volume, path, lpath, stoptime))
		return -1;
	return chirp_reli_setacl(current_volume->host, lpath, subject, rights, stoptime);
}

INT64_T chirp_multi_locate(const char *volume, const char *path, chirp_loc_t callback, void *arg, time_t stoptime)
{
	struct file_info info;
	if(!chirp_multi_lookup(volume, path, &info, stoptime))
		return -1;
	return chirp_reli_locate(info.rhost, info.rpath, callback, arg, stoptime);
}

INT64_T chirp_multi_whoami(const char *volume, char *buf, INT64_T length, time_t stoptime)
{
	char lpath[CHIRP_PATH_MAX];
	if(!chirp_multi_lpath(volume, "/", lpath, stoptime))
		return -1;
	return chirp_reli_whoami(current_volume->host, buf, length, stoptime);
}

INT64_T chirp_multi_unlink(const char *volume, const char *path, time_t stoptime)
{
	struct file_info info;
	int result;
	if(chirp_multi_lookup(volume, path, &info, stoptime)) {
		result = chirp_reli_unlink(info.rhost, info.rpath, stoptime);
		if(result != 0 && errno != ENOENT) {
			debug(D_MULTI, "Unlink file failed: errno=%i (%s)", errno, strerror(errno));
			return -1;
		}

		result = chirp_reli_unlink(current_volume->host, info.lpath, stoptime);
		if(result != 0) {
			debug(D_MULTI, "Unlink stub failed: errno=%i (%s)", errno, strerror(errno));
			return -1;
		}
	} else {
		debug(D_MULTI, "Could not complete volume/path lookup: errno=%i (%s)", errno, strerror(errno));
		return -1;
	}
	return result;
}

INT64_T chirp_multi_rename(const char *volume, const char *path, const char *newpath, time_t stoptime)
{
	char lpath[CHIRP_PATH_MAX];
	char newlpath[CHIRP_PATH_MAX];
	int result;

	if(!chirp_multi_lpath(volume, path, lpath, stoptime))
		return -1;
	if(!chirp_multi_lpath(volume, newpath, newlpath, stoptime))
		return -1;

	result = chirp_multi_unlink(volume, newpath, stoptime);
	if(result < 0) {
		if(errno == ENOENT || errno == EISDIR) {
			/* ok, keep going */
		} else {
			return -1;
		}
	}

	return chirp_reli_rename(current_volume->host, lpath, newlpath, stoptime);
}

INT64_T chirp_multi_link(const char *volume, const char *path, const char *newpath, time_t stoptime)
{
	errno = ENOSYS;
	return -1;
}

INT64_T chirp_multi_symlink(const char *volume, const char *path, const char *newpath, time_t stoptime)
{
	char lpath[CHIRP_PATH_MAX];
	char newlpath[CHIRP_PATH_MAX];
	if(!chirp_multi_lpath(volume, path, lpath, stoptime))
		return -1;
	if(!chirp_multi_lpath(volume, newpath, newlpath, stoptime))
		return -1;
	return chirp_reli_symlink(current_volume->host, lpath, newlpath, stoptime);
}

INT64_T chirp_multi_readlink(const char *volume, const char *path, char *buf, INT64_T length, time_t stoptime)
{
	char lpath[CHIRP_PATH_MAX];
	if(chirp_multi_lpath(volume, path, lpath, stoptime)) {
		return chirp_reli_readlink(current_volume->host, lpath, buf, length, stoptime);
	} else {
		return -1;
	}
}

INT64_T chirp_multi_mkdir(const char *volume, char const *path, INT64_T mode, time_t stoptime)
{
	char lpath[CHIRP_PATH_MAX];
	if(chirp_multi_lpath(volume, path, lpath, stoptime)) {
		return chirp_reli_mkdir(current_volume->host, lpath, mode, stoptime);
	} else {
		return -1;
	}
}

INT64_T chirp_multi_rmdir(const char *volume, char const *path, time_t stoptime)
{
	char lpath[CHIRP_PATH_MAX];
	if(chirp_multi_lpath(volume, path, lpath, stoptime)) {
		return chirp_reli_rmdir(current_volume->host, lpath, stoptime);
	} else {
		return -1;
	}
}

static int emulate_dir_stat(struct chirp_stat *buf)
{
	memset(buf, 0, sizeof(*buf));
	buf->cst_atime = buf->cst_mtime = buf->cst_ctime = time(0);
	buf->cst_mode = S_IFDIR | 0555;
	return 0;
}

INT64_T chirp_multi_stat(const char *volume, const char *path, struct chirp_stat * buf, time_t stoptime)
{
	struct file_info info;

	if(!volume[0]) {
		return emulate_dir_stat(buf);
	} else if(chirp_multi_lookup(volume, path, &info, stoptime)) {
		return chirp_reli_stat(info.rhost, info.rpath, buf, stoptime);
	} else if(errno == EISDIR) {
		return chirp_reli_stat(current_volume->host, info.lpath, buf, stoptime);
	} else {
		return -1;
	}
}

INT64_T chirp_multi_lstat(const char *volume, const char *path, struct chirp_stat * buf, time_t stoptime)
{
	struct file_info info;

	if(!volume[0]) {
		return emulate_dir_stat(buf);
	} else if(chirp_multi_lookup(volume, path, &info, stoptime)) {
		return chirp_reli_lstat(info.rhost, info.rpath, buf, stoptime);
	} else if(errno == EISDIR) {
		return chirp_reli_lstat(current_volume->host, info.lpath, buf, stoptime);
	} else {
		return -1;
	}
}

INT64_T chirp_multi_statfs(const char *volume, const char *path, struct chirp_statfs * buf, time_t stoptime)
{
	struct chirp_server *s;
	INT64_T files_total = 0;
	INT64_T bytes_avail = 0;
	INT64_T bytes_free = 0;
	INT64_T bytes_total = 0;
	INT64_T files_free = 0;
	INT64_T block_size = 4096;
	int i;

	if(!chirp_multi_init(volume, stoptime))
		return 0;

	for(i = 0; i < current_volume->nservers; i++) {
		s = current_volume->servers[i];
		struct chirp_statfs tmp;
		int result;

		result = chirp_reli_statfs(s->name, "/", &tmp, stoptime);
		if(result < 0)
			return result;

		bytes_total += tmp.f_blocks * (INT64_T) tmp.f_bsize;
		bytes_avail += tmp.f_bavail * (INT64_T) tmp.f_bsize;
		bytes_free += tmp.f_bfree * (INT64_T) tmp.f_bsize;
		files_total += tmp.f_files;
		files_free += tmp.f_ffree;
	}

	memset(buf, 0, sizeof(*buf));

	buf->f_bsize = block_size;
	buf->f_blocks = bytes_total / block_size;
	buf->f_bavail = bytes_avail / block_size;
	buf->f_bfree = bytes_free / block_size;
	buf->f_files = files_total;
	buf->f_ffree = files_total;

	return 0;
}

INT64_T chirp_multi_access(const char *volume, const char *path, INT64_T mode, time_t stoptime)
{
	struct file_info info;
	if(chirp_multi_lookup(volume, path, &info, stoptime)) {
		return chirp_reli_access(info.rhost, info.rpath, mode, stoptime);
	} else if(errno == EISDIR) {
		return chirp_reli_access(current_volume->host, info.lpath, mode, stoptime);
	} else {
		return -1;
	}
}

INT64_T chirp_multi_chmod(const char *volume, const char *path, INT64_T mode, time_t stoptime)
{
	struct file_info info;
	if(chirp_multi_lookup(volume, path, &info, stoptime)) {
		return chirp_reli_chmod(info.rhost, info.rpath, mode, stoptime);
	} else if(errno == EISDIR) {
		return chirp_reli_chmod(current_volume->host, info.lpath, mode, stoptime);
	} else {
		return -1;
	}
}

INT64_T chirp_multi_chown(const char *volume, const char *path, INT64_T uid, INT64_T gid, time_t stoptime)
{
	struct file_info info;
	if(chirp_multi_lookup(volume, path, &info, stoptime)) {
		return chirp_reli_chown(info.rhost, info.rpath, uid, gid, stoptime);
	} else if(errno == EISDIR) {
		return chirp_reli_chown(current_volume->host, info.lpath, uid, gid, stoptime);
	} else {
		return -1;
	}
}

INT64_T chirp_multi_lchown(const char *volume, const char *path, INT64_T uid, INT64_T gid, time_t stoptime)
{
	struct file_info info;
	if(chirp_multi_lookup(volume, path, &info, stoptime)) {
		return chirp_reli_lchown(info.rhost, info.rpath, uid, gid, stoptime);
	} else if(errno == EISDIR) {
		return chirp_reli_lchown(current_volume->host, info.lpath, uid, gid, stoptime);
	} else {
		return -1;
	}
}

INT64_T chirp_multi_truncate(const char *volume, const char *path, INT64_T length, time_t stoptime)
{
	struct file_info info;
	if(chirp_multi_lookup(volume, path, &info, stoptime)) {
		return chirp_reli_truncate(info.rhost, info.rpath, length, stoptime);
	} else {
		return -1;
	}
}

INT64_T chirp_multi_utime(const char *volume, const char *path, time_t actime, time_t modtime, time_t stoptime)
{
	struct file_info info;
	if(chirp_multi_lookup(volume, path, &info, stoptime)) {
		return chirp_reli_utime(info.rhost, info.rpath, actime, modtime, stoptime);
	} else if(errno == EISDIR) {
		return chirp_reli_utime(current_volume->host, info.lpath, actime, modtime, stoptime);
	} else {
		return -1;
	}
}

INT64_T chirp_multi_md5(const char *volume, const char *path, unsigned char digest[16], time_t stoptime)
{
	struct file_info info;
	if(chirp_multi_lookup(volume, path, &info, stoptime)) {
		return chirp_reli_md5(info.rhost, info.rpath, digest, stoptime);
	} else {
		return -1;
	}
}

/* vim: set noexpandtab tabstop=4: */
