/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef UNLINK_RECURSIVE_H
#define UNLINK_RECURSIVE_H

/** @file unlink_recursive.h Unlink recursively. */

/** Delete a path recursively.
@param path The path to unlink recursively.
@return 0 on success, -1 on failure.
*/
int unlink_recursive (const char *path);

/** Unlink only the contents of the directory recursively.
@param dirname The path of the directory.
@return 0 on success, -1 on failure.
*/
int unlink_dir_contents (const char *dirname);

#endif
