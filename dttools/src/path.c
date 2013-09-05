/*
 * Copyright (C) 2013- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#include "path.h"

#include "debug.h"
#include "xxmalloc.h"

#include <fcntl.h>

#include <sys/stat.h>

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

void path_absolute (const char *src, char *dest, int exist)
{
	struct stat buf;
    int created = 0;
	if (stat(src, &buf) == -1) {
		if (errno == ENOENT && !exist) {
			/* realpath requires the filename exist, we create the file if necessary */
			if (creat(src, S_IRUSR|S_IWUSR) == -1) {
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
        unlink(src);
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
	const char *dot = strrchr(path, '.');
	if(!dot || dot == path) return "";
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
