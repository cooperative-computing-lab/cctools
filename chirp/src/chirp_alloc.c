/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_alloc.h"
#include "chirp_filesystem.h"
#include "chirp_protocol.h"

#include "debug.h"
#include "hash_table.h"
#include "int_sizes.h"
#include "itable.h"
#include "macros.h"
#include "path.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <sys/stat.h>

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* On why HDFS can't do quotas (allocations) [1]:
 *
 * In the current implementation, quotas (allocations) do not work
 * with HDFS b/c the chirp_alloc module stores the allocation
 * information in the Unix filesystem and relies upon file locking
 * and signals to ensure mutual exclusion. Modifying the code to
 * store it in cfs instead of Unix would be easy, but hdfs still
 * doesn't support file locking. (Nor does any other distributed
 * file system.)
 *
 * An alternative approach would be to store the allocation data in
 * a database alongside the filesystem. This has some pros and cons
 * to be worked out.
 *
 * Addendum: flock is now part of CFS, but HDFS has a stub for it.
 *
 * [1] https://github.com/batrick/cctools/commit/377377f54e7660c8571d3088487b00c8ad2d2d7d#commitcomment-4265178
 */


static int alloc_enabled = 0;
static struct hash_table *alloc_table = 0;
static time_t last_flush_time = 0;
static int recovery_in_progress = 0;
static struct hash_table *root_table = 0;

struct alloc_state {
	int fd;
	INT64_T size;
	INT64_T inuse;
	INT64_T avail;
	INT64_T dirty;
};

/*
Note that the space consumed by a file is not the same
as the filesize.  This function computes the space consumed
by a file of a given size.  Currently, it rounds up to
the next blocksize.  A more exact function might take into
account indirect blocks allocated within the filesystem.
*/

static INT64_T space_consumed(INT64_T filesize)
{
	INT64_T block_size = 4096;
	INT64_T blocks = filesize / block_size;
	if(filesize % block_size)
		blocks++;
	return blocks * block_size;
}

static void alloc_state_update(struct alloc_state *a, INT64_T change)
{
	if(change != 0) {
		a->inuse += change;
		if(a->inuse < 0)
			a->inuse = 0;
		a->avail = a->size - a->inuse;
		a->dirty = 1;
	}
}

static struct alloc_state *alloc_state_load(const char *path)
{
	struct alloc_state *s = xxmalloc(sizeof(*s));
	char statename[CHIRP_PATH_MAX];
	char buffer[4096]; /* any .__alloc file is smaller than this */

	debug(D_ALLOC, "locking %s", path);

	string_nformat(statename, sizeof(statename), "%s/.__alloc", path);

	s->fd = cfs->open(statename, O_RDWR, S_IRUSR|S_IWUSR);
	if(s->fd == -1) {
		free(s);
		return 0;
	}

	if(cfs->lockf(s->fd, F_TLOCK, 0)) {
		debug(D_ALLOC, "lock of %s blocked; flushing outstanding locks", path);
		chirp_alloc_flush();
		debug(D_ALLOC, "locking %s (retry)", path);

		if(cfs->lockf(s->fd, F_LOCK, 0)) {
			debug(D_ALLOC, "lock of %s failed: %s", path, strerror(errno));
			cfs->close(s->fd);
			free(s);
			return 0;
		}
	}

	memset(buffer, 0, sizeof(buffer));
	INT64_T result = cfs->pread(s->fd, buffer, sizeof(buffer), 0);
	assert(0 < result && result < (INT64_T)sizeof(buffer));
	result = sscanf(buffer, "%" SCNd64 " %" SCNd64, &s->size, &s->inuse);
	assert(result == 2);

	s->dirty = 0;

	if(recovery_in_progress) {
		s->inuse = 0;
		s->dirty = 1;
	}

	s->avail = s->size - s->inuse;

	return s;
}

static void alloc_state_save(const char *path, struct alloc_state *s)
{
	if(s->dirty) {
		debug(D_ALLOC, "storing %s", path);
	} else {
		debug(D_ALLOC, "freeing %s", path);
	}

	if(s->dirty) {
		char buffer[4096];
		cfs->ftruncate(s->fd, 0);
		string_nformat(buffer, sizeof(buffer), "%" PRId64 "\n%" PRId64 "\n", s->size, s->inuse);
		int64_t result = cfs->pwrite(s->fd, buffer, strlen(buffer), 0);
		assert(result == (int64_t) strlen(buffer));
	}
	cfs->close(s->fd);
	free(s);
}

static int alloc_state_create(const char *path, INT64_T size)
{
	char statepath[CHIRP_PATH_MAX];
	int fd;

	string_nformat(statepath, sizeof(statepath), "%s/.__alloc", path);
	fd = cfs->open(statepath, O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
	if(fd >= 0) {
		char buffer[4096];
		string_nformat(buffer, sizeof(buffer), "%" PRId64 " 0\n", size);
		int64_t result = cfs->pwrite(fd, buffer, strlen(buffer), 0);
		assert(result == (int64_t) strlen(buffer));
		cfs->close(fd);
		return 1;
	} else {
		return 0;
	}
}

static char *alloc_state_root(const char *path)
{
	char dirname[CHIRP_PATH_MAX];

	string_nformat(dirname, sizeof(dirname), "%s", path);

	while(1) {
		char statename[CHIRP_PATH_MAX];
		string_nformat(statename, sizeof(statename), "%s/.__alloc", dirname);
		if(cfs_file_size(statename) >= 0) {
			if (dirname[0])
				return xxstrdup(dirname);
			else
				return xxstrdup("/");
		}
		char *s = strrchr(dirname, '/');
		if(!s)
			return 0;
		*s = 0;
	}

	return 0;
}

static char *alloc_state_root_cached(const char *path)
{
	char *result;

	result = hash_table_lookup(root_table, path);
	if(result)
		return result;

	result = alloc_state_root(path);
	if(!result)
		return 0;

	hash_table_insert(root_table, path, result);

	return result;
}

static struct alloc_state *alloc_state_cache_exact(const char *path)
{
	struct alloc_state *a;
	char *d;
	char dirname[CHIRP_PATH_MAX];

	d = alloc_state_root_cached(path);
	if(!d)
		return 0;

	/*
	   Save a copy of dirname, because the following
	   alloc_table_load_cached may result in a flush of the alloc table root.
	 */
	strcpy(dirname, d);
	a = hash_table_lookup(alloc_table, dirname);
	if(a)
		return a;

	a = alloc_state_load(dirname);
	if(!a)
		return a;

	hash_table_insert(alloc_table, dirname, a);

	return a;
}

static struct alloc_state *alloc_state_cache(const char *path)
{
	char dirname[CHIRP_PATH_MAX];
	path_dirname(path, dirname);
	return alloc_state_cache_exact(dirname);
}

static void recover(const char *path)
{
	char newpath[CHIRP_PATH_MAX];
	struct alloc_state *a, *b;
	struct chirp_dir *dir;
	struct chirp_dirent *d;

	a = alloc_state_cache_exact(path);
	if(!a)
		fatal("couldn't open alloc state in %s: %s", path, strerror(errno));

	dir = cfs->opendir(path);
	if(!dir)
		fatal("couldn't open %s: %s\n", path, strerror(errno));

	while((d = cfs->readdir(dir))) {
		if(!strcmp(d->name, "."))
			continue;
		if(!strcmp(d->name, ".."))
			continue;
		if(!strncmp(d->name, ".__", 3))
			continue;

		string_nformat(newpath, sizeof(newpath), "%s/%s", path, d->name);

		if(S_ISDIR(d->info.cst_mode)) {
			recover(newpath);
			b = alloc_state_cache_exact(newpath);
			if(a != b)
				alloc_state_update(a, b->size);
		} else if(S_ISREG(d->info.cst_mode)) {
			alloc_state_update(a, space_consumed(d->info.cst_size));
		} else {
			debug(D_ALLOC, "warning: unknown file type: %s\n", newpath);
		}
	}

	cfs->closedir(dir);

	debug(D_ALLOC, "%s (%sB)", path, string_metric(a->inuse, -1, 0));
}

int chirp_alloc_init(INT64_T size)
{
	struct alloc_state *a;
	time_t start, stop;
	INT64_T inuse, avail;

	alloc_enabled = 0;
	if(size == 0) {
		return 0;
	} else if (cfs->lockf(-1, F_TEST, 0) == -1 && errno == ENOSYS) {
		return -1;
	}
#ifdef CCTOOLS_OPSYS_CYGWIN
	fatal("sorry, CYGWIN cannot employ space allocation because it does not support file locking.");
#endif

	alloc_enabled = 1;
	recovery_in_progress = 1;

	assert(alloc_table == NULL);
	alloc_table = hash_table_create(0, 0);
	assert(root_table == NULL);
	root_table = hash_table_create(0, 0);

	debug(D_ALLOC, "### begin allocation recovery scan ###");

	if(!alloc_state_create("/", size)) {
		debug(D_ALLOC, "couldn't create allocation in `/': %s\n", strerror(errno));
		return -1;
	}

	a = alloc_state_cache_exact("/");
	if(!a) {
		debug(D_ALLOC, "couldn't find allocation in `/': %s\n", strerror(errno));
		return -1;
	}

	start = time(0);
	recover("/");
	size = a->size;
	inuse = a->inuse;
	avail = a->avail;
	chirp_alloc_flush();
	stop = time(0);

	debug(D_ALLOC, "### allocation recovery took %d seconds ###", (int) (stop-start) );

	debug(D_ALLOC, "%sB total", string_metric(size, -1, 0));
	debug(D_ALLOC, "%sB in use", string_metric(inuse, -1, 0));
	debug(D_ALLOC, "%sB available", string_metric(avail, -1, 0));

	recovery_in_progress = 0;
	return 0;
}

void chirp_alloc_flush()
{
	char *path, *root;
	struct alloc_state *s;

	if(!alloc_enabled)
		return;

	debug(D_ALLOC, "flushing allocation states...");


	hash_table_firstkey(alloc_table);
	while(hash_table_nextkey(alloc_table, &path, (void **) &s)) {
		alloc_state_save(path, s);
		hash_table_remove(alloc_table, path);
	}


	hash_table_firstkey(root_table);
	while(hash_table_nextkey(root_table, &path, (void **) &root)) {
		free(root);
		hash_table_remove(root_table, path);
	}

	last_flush_time = time(0);
}

int chirp_alloc_flush_needed()
{
	if(!alloc_enabled)
		return 0;
	return hash_table_size(alloc_table);
}

time_t chirp_alloc_last_flush_time()
{
	return last_flush_time;
}

INT64_T chirp_alloc_realloc (const char *path, INT64_T change, INT64_T *current)
{
	struct alloc_state *a;
	int result;
	INT64_T dummy;

	if (current == NULL)
		current = &dummy;

	if(!alloc_enabled) {
		*current = 0;
		return 0;
	}

	debug(D_ALLOC, "path `%s' change = %" PRId64, path, change);
	a = alloc_state_cache(path);
	if(a) {
		/* FIXME this won't work with symlinks, problem existed before probably */
		*current = cfs_file_size(path);
		if(*current == -1 && errno == ENOENT)
			*current = 0;
		if(*current >= 0) {
			if(change == *current) {
				/* NOP */
				result = 0;
			} else {
				INT64_T alloc_change = space_consumed(change) - space_consumed(*current);
				debug(D_ALLOC, "path `%s' actual change = %" PRId64 " from current = %" PRId64, path, alloc_change, *current);
				if(a->avail >= alloc_change) {
					alloc_state_update(a, alloc_change);
					result = 0;
				} else {
					errno = ENOSPC;
					result = -1;
				}
			}
		} else {
			result = -1;
		}
	} else {
		result = -1;
	}
	return result;
}

INT64_T chirp_alloc_frealloc (int fd, INT64_T change, INT64_T *current)
{
	char path[CHIRP_PATH_MAX];
	if (cfs->fname(fd, path) == -1) return -1;
	return chirp_alloc_realloc(path, change, current);
}

INT64_T chirp_alloc_statfs(const char *path, struct chirp_statfs * info)
{
	struct alloc_state *a;
	int result;

	if(!alloc_enabled)
		return cfs->statfs(path, info);

	a = alloc_state_cache(path);
	if(a) {
		result = cfs->statfs(path, info);
		if(result == 0) {
			info->f_blocks = a->size / info->f_bsize;
			info->f_bavail = a->avail / info->f_bsize;
			info->f_bfree = a->avail / info->f_bsize;
			if(a->avail < 0) {
				info->f_bavail = 0;
				info->f_bfree = 0;
			}
		}
	} else {
		result = -1;
	}

	return result;
}

INT64_T chirp_alloc_fstatfs(int fd, struct chirp_statfs *buf)
{
	char path[CHIRP_PATH_MAX];
	if (cfs->fname(fd, path) == -1) return -1;
	return chirp_alloc_statfs(path, buf);
}

INT64_T chirp_alloc_lsalloc(const char *path, char *alloc_path, INT64_T * total, INT64_T * inuse)
{
	int result = -1;

	if(!alloc_enabled) {
		errno = ENOSYS;
		return -1;
	}

	char *name = alloc_state_root_cached(path);
	if(name) {
		struct alloc_state *a = alloc_state_cache_exact(name);
		if(a) {
			strcpy(alloc_path, name);
			*total = a->size;
			*inuse = a->inuse;
			result = 0;
		} else {
			result = -1;
		}
	} else {
		result = -1;
	}
	return result;
}

INT64_T chirp_alloc_mkalloc(const char *path, INT64_T size, INT64_T mode)
{
	struct alloc_state *a;
	int result = -1;

	if(!alloc_enabled) {
		errno = ENOSYS;
		return -1;
	}

	a = alloc_state_cache(path);
	if(a) {
		if(a->avail > size) {
			result = cfs->mkdir(path, mode);
			if(result == 0) {
				if(alloc_state_create(path, size)) {
					alloc_state_update(a, size);
					debug(D_ALLOC, "mkalloc %s %"PRId64, path, size);
					chirp_alloc_flush();
				} else {
					result = -1;
				}
			}
		} else {
			errno = ENOSPC;
			return -1;
		}
	} else {
		return -1;
	}

	return result;
}

/* vim: set noexpandtab tabstop=4: */
