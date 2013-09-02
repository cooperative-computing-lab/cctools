/*
 * Copyright (C) 2013- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef PATH_H
#define PATH_H

void path_absolute (const char *src, char *dest, int exist);
const char *path_basename (const char * path);
void path_collapse (const char *l, char *s, int remove_dotdot);
void path_dirname (const char *path, char *dir);

/** Returns a heap allocated freeable string for the current working directory.
 *  @return The current working directory.
 */
char *path_getcwd (void);

void path_remove_trailing_slashes (char *path);
void path_split (const char *input, char *first, char *rest);
void path_split_multi (const char *input, char *first, char *rest);

#endif
