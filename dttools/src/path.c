/*
 * Copyright (C) 2013- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#include "path.h"

#include "buffer.h"
#include "debug.h"
#include "xxmalloc.h"

#include <dirent.h>
#include <fcntl.h>
#include <fnmatch.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

void path_absolute (const char *src, char *dest, int exist)
{
	struct stat buf;
	int created = 0;
	if (stat(src, &buf) == -1) {
		/* realpath requires the filename exist, we create the file if necessary */
		if (errno == ENOENT && !exist) {
			/* We use mkdir because src may end with forward slashes. */
			if (mkdir(src, S_IRUSR|S_IWUSR) == -1) {
				fatal("generating absolute path to `%s': %s", src, strerror(errno));
			}
            created = 1;
		} else {
			fatal("could not resolve path `%s': %s", src, strerror(errno));
		}
	}
	if (realpath(src, dest) == NULL) {
		fatal("could not resolve path `%s': %s", src, strerror(errno));
	}
    if (created) {
		if (rmdir(src) == -1)
			fatal("could not delete temporary dir `%s': %s", src, strerror(errno));
    }
}

/* The interface here is actually bad. A typical basename implementation should
 * remove final trailing slashes for the final component. This function only
 * returns the first character of the final component in `path', which may have
 * trailing slashes.
 */
const char *path_basename (const char *path)
{
	const char *base;
	size_t len = strlen(path);

	if (len == 0)
		return ".";

	/* skip trailing slashes */
	for (base = path+len-1; base > path && *base == '/'; base--) ;

	/* find final component's first character */
	while (base > path && base[-1] != '/') base--;

	return base;
}

/* Returns the filename extension.
 *
 * To extract multiple extensions (e.g. .tar.gz) call path_extension multiple
 * times and concatenate the results.
 */
const char *path_extension (const char *path)
{
	const char *base = path_basename(path);
	const char *dot = strrchr(base, '.');
	if(!dot || dot == base) return NULL;
	return dot + 1;
}

/* Canonicalize a pathname by stripping out duplicate slashes and redundant
 * dots.
 *
 * A trailing slash is semantically important [1] in pathname resolution
 * because it forces resolution of the final component and that final component
 * must resolve to a directory. So, we allow a final trailing slash in
 * the canonical path.
 *
 * [1] http://pubs.opengroup.org/onlinepubs/009695399/basedefs/xbd_chap04.html
*/
void path_collapse (const char *l, char *s, int remove_dotdot)
{
	char *start = s;

	while(*l) {
		if((*l) == '/' && (*(l + 1)) == '/') {
			l++;	/* skip double slash */
		} else if((*l) == '/' && (*(l + 1)) == '.' && (*(l + 2)) == 0) {
			*s++ = *l++;
			break;
		} else if((*l) == '/' && (*(l + 1)) == '.' && (*(l + 2)) == '/') {
			l += 2;
		} else if(remove_dotdot && !strncmp(l, "/..", 3) && (l[3] == 0 || l[3] == '/')) {
			if(s > start)
				s--;
			while(s > start && ((*s) != '/')) {
				s--;
			}
			*s = 0;
			l += 3;
		} else {
			*s++ = *l++;
		}
	}

	*s = 0;

	if(s == start) strcpy(s, "/");

	/* canonicalize certain final components */
	if(strcmp(start, "./") == 0) strcpy(start, ".");
	if(strcmp(start, "../") == 0) strcpy(start, "..");
	if((s-start) > 4 && strcmp(s-4, "/../") == 0) *(s-1) = 0;
}

void path_dirname (const char *path, char *dir)
{
	char *c;

	strcpy(dir, path);

	/* A trailing slash is semantically important [1] in pathname resolution
	 * because it forces resolution of the final component and that final
	 * component must resolve to a directory. Here, we remove final slashes so
	 * we can get at the containing directory for the final component.
	 *
	 * [1] http://pubs.opengroup.org/onlinepubs/009695399/basedefs/xbd_chap04.html
	 */
	path_remove_trailing_slashes(dir);

	/* This will be the (possibly one of many) trailing slash for the
	 * containing directory.
	 */
	c = strrchr(dir, '/');
	if(c) {
		/* remove all trailing (redundant) slashes */
		for (; c >= dir && *c == '/'; c--)
			*c = 0;
		if(dir[0] == 0)
			strcpy(dir, "/");
	} else {
		strcpy(dir, ".");
	}
}

int path_lookup (char *search_path, const char *exe, char *dest, size_t destlen)
{
	char *s;
	char *e;
	size_t len = strlen(search_path);

	s = e = search_path;

	while(e < search_path+len) {
		DIR *dirp = NULL;

		while(*e != ':' && *e != '\0') e++;
		*e = '\0';

		if( *s != '/' ){
			char tmp[PATH_MAX];
			char *cwd;
			cwd = path_getcwd();
			snprintf(tmp, PATH_MAX, "%s/%s", cwd, s);
			free(cwd);
			s = tmp;
		}

		if(( dirp = opendir(s) )) {
			struct dirent *dp = NULL;
			while(( dp = readdir(dirp) )) {
				if( strcmp(dp->d_name, exe) == 0 ) {
					struct stat sb;
					char fn[PATH_MAX];
					strncpy(fn, s, PATH_MAX);
					strncat(fn, "/", 1);
					strcat(fn, dp->d_name);
					if( stat(fn, &sb) == 0 && sb.st_mode & (S_IXUSR|S_IFREG) ){
						strncpy(dest, fn, destlen);
						closedir(dirp);
						return 0;
					}
				}
			}
			closedir(dirp);
		}
		*e = ':';
		e++;
		s = e;
	}

	return 1;
}

char *path_getcwd (void)
{
	char *result = NULL;
	size_t size = PATH_MAX;
	result = xxrealloc(result, size);

	while(getcwd(result, size) == NULL) {
		if(errno == ERANGE) {
			size *= 2;
			result = xxrealloc(result, size);
		} else {
			fatal("couldn't getcwd: %s", strerror(errno));
			return NULL;
		}
	}
	return result;
}

void path_remove_trailing_slashes (char *path)
{
	char *s;

	/* Note: Loop body is never run where s == path (never removes first char)
	 * Consequently Note: if path == `/' then loop body never executes.
	 */
	for (s = path+strlen(path)-1; s > path && *s == '/'; s--) {
		*s = '\0';
	}
}

void path_split (const char *input, char *first, char *rest)
{
	/* skip any leading slashes */
	while(*input == '/') {
		input++;
	}

	/* copy the first element up to slash or null */
	while(*input && *input != '/') {
		*first++ = *input++;
	}
	*first = 0;

	/* make sure that rest starts with a slash */
	if(*input != '/') {
		*rest++ = '/';
	}

	/* copy the rest */
	while(*input) {
		*rest++ = *input++;
	}
	*rest = 0;
}

void path_split_multi (const char *input, char *first, char *rest)
{
	/* skip any leading slashes */
	while(*input == '/') {
		input++;
	}

	/* copy the first element up to slash or @ or null */
	while(*input && *input != '/' && *input != '@') {
		*first++ = *input++;
	}
	*first = 0;

	/* make sure that rest starts with a slash or @ */
	if(*input != '/' && *input != '@') {
		*rest++ = '/';
	}

	/* copy the rest */
	while(*input) {
		*rest++ = *input++;
	}
	*rest = 0;
}

static int find (buffer_t *B, const size_t base, buffer_t *path, const char *pattern, int recursive)
{
	int rc = 0;
	DIR *D = opendir(buffer_tostring(path, NULL));
	if (D) {
		struct dirent *entry;
		size_t current = buffer_pos(path);
		while ((entry = readdir(D))) {
			struct stat buf;

			if (buffer_putstring(path, entry->d_name) == -1) goto failure;
			/* N.B. We don't use FNM_PATHNAME, so `*.c' matches `foo/bar.c' */
			if (fnmatch(pattern, buffer_tostring(path, NULL)+base, 0) == 0) {
				if (buffer_printf(B, "%s%c", buffer_tostring(path, NULL), 0) == -1) goto failure; /* NUL padded */
				rc += 1;
			}
			if (recursive && strcmp(entry->d_name, ".") && strcmp(entry->d_name, "..") && stat(buffer_tostring(path, NULL), &buf) == 0 && S_ISDIR(buf.st_mode)) {
				if (buffer_putliteral(path, "/") == -1) goto failure;
				int found = find(B, base, path, pattern, recursive);
				if (found == -1)
					goto failure;
				else if (found > 0)
					rc += found;
			}
			buffer_rewind(path, current);
		}
	} /* else skip */
	goto out;
failure:
	rc = -1;
	goto out;
out:
	if (D)
		closedir(D);
	return rc;
}

int path_find (buffer_t *B, const char *dir, const char *pattern, int recursive)
{
	int rc=0;
	buffer_t path;
	buffer_init(&path);
	if (buffer_printf(&path, "%s/", dir) == -1) goto out;
	rc = find(B, buffer_pos(&path), &path, pattern, recursive);
out:
	buffer_free(&path);
	return rc;
}

/* vim: set noexpandtab tabstop=4: */
