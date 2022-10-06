/*
 * Copyright (C) 2022 The University of Notre Dame
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

int path_within_dir( const char *path, const char *dir );


/*
Returns the first absolute path for executable exec as found in PATH.
Returns NULL if none is found.
*/
char *path_which(const char *exec);

/* path_join_two_strings joins two strings, and adds sep between them.
 * The caller should free the space pointed by the returned pointer.
 */
char *path_join_two_strings(const char *s, const char *t, const char * sep);

/* path_concat concatenates two file paths, with a slash as the separator.
 * @param s: a file path
 * @param t: a file path
 * @return p: return the concatenated string on success, return NULL on failure.
 * The caller should free the returned string.
 */
char *path_concat(const char *s, const char *t);

/* path_has_symlink checks whether any level of a path is symbolic link.
 * return 0 if all the levels of the path are not symlink; otherwise return non-zero.
 */
int path_has_symlink(const char *s);

/* path_has_doubledots checks whether s includes double dots to reference a parent directory.
 * if s looks like "a/../b", return 1; if s looks like "a/b..b/c", return 0.
 */
int path_has_doubledots(const char *s);

/* path_depth checks the depth of a path. The path should not include double dots.
 * If s is an absolute path, return the depth of the path relative to /.
 * If s is a relative path, return the depth of the path relative to CWD.
 * return the depth of the path.
 */
int path_depth(const char *s);

/** Finds if the name of some file is a directory
@param file_name The directory that will be checked
@return 1 if readable directory, 0 otherwise
 */
int path_is_dir(char *file_name);


#endif
