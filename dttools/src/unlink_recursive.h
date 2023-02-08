/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef UNLINK_RECURSIVE_H
#define UNLINK_RECURSIVE_H

#include "compat-at.h"

#include <fcntl.h>

/** @file unlink_recursive.h Unlink recursively. */

/** Delete a path recursively.
@param fd Open directory.
@param path The path relative to the open directory fd to unlink recursively.
@return 0 on success, -1 on failure.
*/
int unlinkat_recursive (int fd, const char *path);

/** Delete a path recursively.
@param path The path to unlink recursively.
@return 0 on success, -1 on failure.
*/
int unlink_recursive (const char *path);

/** Unlink only the contents of the directory recursively.
@param path The path of the directory.
@return 0 on success, -1 on failure.
*/
int unlink_dir_contents (const char *path);

#endif
