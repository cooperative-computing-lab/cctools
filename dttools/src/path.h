/*
 * Copyright (C) 2013- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef PATH_H
#define PATH_H

#include "buffer.h"

void path_absolute (const char *src, char *dest, int exist);
const char *path_basename (const char * path);
const char *path_extension (const char *path);
void path_collapse (const char *l, char *s, int remove_dotdot);
void path_dirname (const char *path, char *dir);

/** Lookup exe in search path.
 * @param search_path Colon separated string of directories.
 * @param exe Name of executable to search for.
 * @param dest Location for absolute path of executable.
 * @param destlen Length of destination buffer.
 * @return 0 on success, non-zero if not found.
 */
int path_lookup (char *search_path, const char *exe, char *dest, size_t destlen);

/** Returns a heap allocated freeable string for the current working directory.
 *  @return The current working directory.
 */
char *path_getcwd (void);

void path_remove_trailing_slashes (char *path);
void path_split (const char *input, char *first, char *rest);
void path_split_multi (const char *input, char *first, char *rest);

int path_find (buffer_t *B, const char *dir, const char *pattern, int recursive);

#endif
