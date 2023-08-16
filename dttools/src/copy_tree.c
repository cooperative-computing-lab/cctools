/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <libgen.h>
#include <limits.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "copy_tree.h"
#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "path.h"
#include "xxmalloc.h"

static mode_t default_dirmode = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;

int copy_dir_real(const char *source, const char *target);

int copy_symlink(const char *source, const char *target) {
	char linkname[PATH_MAX];
	ssize_t r;

	assert(source);
	assert(target);

	if(!access(target, F_OK)) {
		debug(D_DEBUG, "%s already exists!\n", target);
		return -1;
	}

	r = readlink(source, linkname, sizeof(linkname));
	if(r == -1) {
		debug(D_DEBUG, "readlink(`%s`) failed: %s!\n", source, strerror(errno));
		return -1;
	}
	linkname[r] = '\0';

	if(symlink(linkname, target)) {
		debug(D_DEBUG, "symlink(`%s`, `%s`) failed: %s\n", linkname, target, strerror(errno));
		return -1;
	}
	return 0;
}

/* return 0 on success, return -1 on failure. */
int copy_direntry(const char *s, const char *t) {
	struct stat s_stat;

	assert(s);
	assert(t);

	if(lstat(s, &s_stat)) {
		debug(D_DEBUG, "lstat(`%s`): %s\n", s, strerror(errno));
		return -1;
	}

	if(S_ISDIR(s_stat.st_mode)) {
		if(!create_dir(t, (int)default_dirmode)) return -1;
		return copy_dir_real(s, t);
	} else if(S_ISREG(s_stat.st_mode)) {
		return copy_file_to_file(s, t);
	} else if(S_ISLNK(s_stat.st_mode)) {
		return copy_symlink(s, t);
	} else {
		debug(D_DEBUG, "Ignore Copying %s: only dir, regular files, and symlink are supported!\n", s);
		return -1;
	}
}

/* copy_dir_real copies from source to target.
 * @param source: a file path.
 * @param target: a file path which already exists.
 * return 0 on success, return -1 on failure.
 */
int copy_dir_real(const char *source, const char *target) {
	DIR *dir;
	struct dirent *entry;
	int rc = 0;

	assert(source);
	assert(target);

	if((dir = opendir(source)) == NULL) {
		debug(D_DEBUG, "opendir(`%s`) failed: %s!\n", source, strerror(errno));
		return -1;
	}

	while((entry = readdir(dir)) != NULL)
	{
		char *s = NULL;
		char *t = NULL;

		if(!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
			continue;

		s = path_concat(source, entry->d_name);
		if(!s) {
			rc = -1;
			goto finish;
		}

		t = path_concat(target, entry->d_name);
		if(!t) {
			free(s);
			rc = -1;
			goto finish;
		}

		if (copy_direntry(s, t) == -1) {
			free(s);
			free(t);
			goto finish;
		}
		free(s);
		free(t);
	}

finish:
	if(closedir(dir)) {
		debug(D_DEBUG, "closedir(`%s`) failed: %s!", source, strerror(errno));
		rc = -1;
	}
	return rc;
}

int copy_dir(const char *source, const char *target) {
	int rc = 0;

	assert(source);
	assert(target);

	if(!access(target, F_OK)) {
		/* the target already exists */

		char *source_copy = NULL;
		char *source_basename = NULL;
		char *t = NULL;

		// get the basename of the source
		source_copy = xxstrdup(source);
		path_remove_trailing_slashes((char *)source_copy);
		source_basename = basename(source_copy);

		/* create target/basename(source) */
		t = path_concat(target, source_basename);
		if(!t) {
			rc = -1;
			goto finish;
		}

		if(!access(t, F_OK)) {
			debug(D_DEBUG, "%s already exists!\n", t);
			rc = -1;
			goto finish;
		}

		if(!create_dir(t, (int)default_dirmode) || copy_dir_real(source, t)) {
			rc = -1;
			goto finish;
		}

finish:
		free(source_copy);
		free(t);
	} else {
		/* the target does not exist */
		if(!create_dir(target, (int)default_dirmode) || copy_dir_real(source, target)) {
			rc = -1;
		}
	}
	return rc;
}

file_type check_file_type(const char *source) {
	assert(source);

	struct stat st;
	if(lstat(source, &st)) {
		debug(D_DEBUG, "lstat(`%s`) failed: %s!\n", source, strerror(errno));
		return -1;
	}

	if(S_ISREG(st.st_mode)) {
		return FILE_TYPE_REG;
	} else if(S_ISLNK(st.st_mode)) {
		return FILE_TYPE_LNK;
	} else if(S_ISDIR(st.st_mode)) {
		return FILE_TYPE_DIR;
	} else {
		debug(D_DEBUG, "the file type of %s is not supported: only dir, regular files, and symlink are supported!\n", source);
		return FILE_TYPE_UNSUPPORTED;
	}
}

char *get_exist_ancestor_dir(const char *s) {
	char *p, *q;
	char *r;

	/* the function needs to modify the string, so make a copy and modify the copied version */
	q = xxstrdup(s);

	p = q;

	while(*p) {
		char old_p;
		size_t n = strspn(p, "/") + strcspn(p, "/");

		p += n;
		old_p = *p;
		*p = '\0';

		if(access(q, F_OK)) {
			*p = old_p;
			p -= n;
			break;
		}

		*p = old_p;
	}

	r = malloc(sizeof(char) * (p-q+1));
	if(!r) {
		debug(D_DEBUG, "malloc failed: %s!\n", strerror(errno));
		free(q);
		return NULL;
	}

	snprintf(r, p-q+1, "%s", q);
	free(q);
	return r;
}

int is_subdir(const char *source, const char *target) {
	char *t;
	char *s_real, *t_real;
	size_t ns, nt;

	t = get_exist_ancestor_dir(target);
	if(!t) return -1;

	s_real = realpath(source, NULL);
	if(!s_real) {
		debug(D_DEBUG, "realpath(`%s`) failed: %s!\n", source, strerror(errno));
		return -1;
	}

	if(t[0] == '\0') /* if t is an empty string, it means target is a relative path, and no any part of target exists. */
		t_real = realpath(".", NULL);
	else
		t_real = realpath(t, NULL);
	if(!t_real) {
		debug(D_DEBUG, "realpath(`%s`) failed: %s!\n", t, strerror(errno));
		return -1;
	}

	ns = strlen(s_real);
	nt = strlen(t_real);

	if(ns <= nt && !strncmp(s_real, t_real, ns)) {
		free(t);
		free(s_real);
		free(t_real);
		return -1;
	}

	free(t);
	free(s_real);
	free(t_real);
	return 0;
}

/* vim: set noexpandtab tabstop=8: */
