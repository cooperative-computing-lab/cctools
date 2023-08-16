/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "file_cache.h"
#include "create_dir.h"
#include "hash_table.h"
#include "debug.h"
#include "md5.h"
#include "domain_name_cache.h"

#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <dirent.h>
#include <limits.h>
#include <signal.h>
#include <time.h>

/* Cygwin does not have 64-bit I/O, while Darwin has it by default. */

#ifdef CCTOOLS_OPSYS_DARWIN
#define fstat64 fstat
#define stat64 stat
#define open64 open
#define mkstemp64 mkstemp
#endif

struct file_cache {
	char *root;
};

static void cached_name(struct file_cache *c, const char *path, char *lpath)
{
	unsigned char digest[MD5_DIGEST_LENGTH];
	md5_buffer(path, strlen(path), digest);
	sprintf(lpath, "%s/%02x/%s", c->root, digest[0], md5_to_string(digest));
}

static void txn_name(struct file_cache *c, const char *path, char *txn)
{
	unsigned char digest[MD5_DIGEST_LENGTH];
	char shortname[DOMAIN_NAME_MAX];
	domain_name_cache_guess_short(shortname);
	md5_buffer(path, strlen(path), digest);
	sprintf(txn, "%s/txn/%s.%s.%d.XXXXXX", c->root, md5_to_string(digest), shortname, (int) getpid());
}

static int wait_for_running_txn(struct file_cache *c, const char *path)
{
	char txn[PATH_MAX];
	char dirname[PATH_MAX];
	unsigned char digest[MD5_DIGEST_LENGTH];
	DIR *dir;
	struct dirent *d;
	const char *checksum;

	md5_buffer(path, strlen(path), digest);
	checksum = md5_to_string(digest);

	txn[0] = 0;

	sprintf(dirname, "%s/txn", c->root);

	dir = opendir(dirname);
	if(!dir)
		return 0;

	while((d = readdir(dir))) {
		if(!strncmp(d->d_name, checksum, 32)) {
			sprintf(txn, "%s/txn/%s", c->root, d->d_name);
			break;
		}
	}

	closedir(dir);

	if(!txn[0])
		return 0;

	while(1) {
		struct stat64 info;

		debug(D_CACHE, "wait %s", txn);
		if(stat64(txn, &info) < 0)
			return 1;

		time_t current = time(0);

		if((current - info.st_mtime) < 60) {
			sleep(1);
			continue;
		} else {
			debug(D_CACHE, "override %s", txn);
			return 0;
		}
	}

}

static int mkdir_or_exists( const char *path, mode_t mode )
{
	return mkdir(path,mode)==0 || errno==EEXIST;
}

struct file_cache *file_cache_init(const char *root)
{
	char path[PATH_MAX];
	struct stat64 buf;
	int result, i;

	struct file_cache *f = malloc(sizeof(*f));
	if(!f)
		return 0;

	f->root = strdup(root);
	if(!f->root) {
		free(f);
		return 0;
	}

	sprintf(path, "%s/ff", root);
	result = stat64(path, &buf);
	if(result != 0) {
		debug(D_CACHE, "%s does not exist, creating cache directories...", path);
		if(!create_dir(path, 0777))
			goto failure;
		sprintf(path, "%s/txn", root);
		if(!mkdir_or_exists(path,0777))
			goto failure;
		for(i = 0; i <= 0xff; i++) {
			sprintf(path, "%s/%02x", root, i);
			if(!mkdir_or_exists(path,0777))
				goto failure;
		}
	}

	return f;

	  failure:
	file_cache_fini(f);
	return 0;
}

void file_cache_fini(struct file_cache *f)
{
	if(f) {
		free(f->root);
		free(f);
	}
}

void file_cache_cleanup(struct file_cache *f)
{
	char path[PATH_MAX];
	struct dirent *d;
	DIR *dir;

	char shortname[DOMAIN_NAME_MAX];
	char myshortname[DOMAIN_NAME_MAX];

	domain_name_cache_guess_short(myshortname);

	sprintf(path, "%s/txn", f->root);

	dir = opendir(path);
	if(!dir)
		return;

	debug(D_CACHE, "cleaning up cache directory %s", f->root);

	while((d = readdir(dir))) {
		int pid;
		if(!strcmp(d->d_name, "."))
			continue;
		if(!strcmp(d->d_name, ".."))
			continue;
		if(sscanf(d->d_name, "%*[^.].%[^.].%d", shortname, &pid) == 2) {
			if(!strcmp(shortname, myshortname)) {
				if(kill(pid, 0) == 0) {
					debug(D_CACHE, "keeping  %s (process alive)", d->d_name);
				} else if(errno == ESRCH) {
					debug(D_CACHE, "deleting %s (process gone)", d->d_name);
					sprintf(path, "%s/txn/%s", f->root, d->d_name);
					unlink(path);
				} else {
					debug(D_CACHE, "ignoring %s (unknown process)", d->d_name);
				}
			} else {
				debug(D_CACHE, "ignoring %s (other host)", d->d_name);
			}
		} else {
			debug(D_CACHE, "ignoring %s (unknown format)", d->d_name);
		}
	}

	closedir(dir);
}

int file_cache_stat(struct file_cache *c, const char *path, char *lpath, struct stat64 *info)
{
	cached_name(c, path, lpath);

	if(stat64(lpath, info) == 0)
		return 0;

	if(wait_for_running_txn(c, path)) {
		if(stat64(lpath, info) == 0)
			return 0;
	}

	return -1;
}

int file_cache_contains(struct file_cache *c, const char *path, char *lpath)
{
	struct stat64 info;
	if(file_cache_stat(c, path, lpath, &info) == 0) {
		return 0;
	} else {
		return -1;
	}
}

int file_cache_open(struct file_cache *c, const char *path, int flags, char *lpath, INT64_T size, time_t mtime)
{
	int fd;
	cached_name(c, path, lpath);

	flags &= (O_RDONLY|O_WRONLY|O_RDWR);

	debug(D_DEBUG, "open('%s', %d)", lpath, flags);
	fd = open64(lpath, flags, 0);
	if (fd == -1) {
		debug(D_DEBUG, "waiting for txn('%s')", path);
		if(!wait_for_running_txn(c, path))
			return -1;
		fd = open64(lpath, flags, 0);
	}

	if (fd >= 0) {
		struct stat64 info;

		if (fstat64(fd, &info) == 0) {
			if((size == 0 || (size == info.st_size)) && ((mtime == 0) || (info.st_mtime >= mtime))) {
				debug(D_CACHE, "hit %s %s", path, lpath);
				return fd;
			} else {
				debug(D_CACHE, "stale %s %s", path, lpath);
				close(fd);
				errno = ENOENT;
				return -1;
			}
		} else {
			int s = errno;
			close(fd);
			errno = s;
			return -1;
		}
	} else {
		debug(D_CACHE, "miss %s %s", path, lpath);
		return -1;
	}
}

int file_cache_delete(struct file_cache *f, const char *path)
{
	char lpath[PATH_MAX];
	cached_name(f, path, lpath);
	debug(D_CACHE, "remove %s %s", path, lpath);
	return unlink(lpath);
}

int file_cache_begin(struct file_cache *f, const char *path, char *txn)
{
	int result;
	txn_name(f, path, txn);
	result = mkstemp64(txn);
	if(result >= 0) {
		debug(D_CACHE, "begin %s %s", path, txn);
		fchmod(result, 0700);
	}
	return result;
}

int file_cache_abort(struct file_cache *f, const char *path, const char *txn)
{
	debug(D_CACHE, "abort %s %s", path, txn);
	return unlink(txn);
}

int file_cache_commit(struct file_cache *f, const char *path, const char *txn)
{
	int result;
	char lpath[PATH_MAX];
	cached_name(f, path, lpath);
	debug(D_CACHE, "commit %s %s %s", path, txn, lpath);
	result = rename(txn, lpath);
	if(result < 0)
		debug(D_CACHE, "commit failed: %s", strerror(errno));
	return result;
}

/* vim: set noexpandtab tabstop=8: */
