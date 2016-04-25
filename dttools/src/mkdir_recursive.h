/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MKDIR_RECURSIVE_H
#define MKDIR_RECURSIVE_H

#include "compat-at.h"

#include <fcntl.h>
#include <sys/stat.h>

/** @file mkdir_recursive.h Create a new directory recursively. */

/** Create a new directory recursively.
@param fd The directory path is relative to.
@param path The full path of a directory.  It is not necessary for all components of the path to exist.
@param mode The desired unix mode bits of the directory and parents.
@return -1 on failure, 0 on success.
*/

int mkdirat_recursive(int fd, const char *path, mode_t mode);

/** Create a new directory recursively.
@param path The full path of a directory.  It is not necessary for all components of the path to exist.
@param mode The desired unix mode bits of the directory and parents.
@return -1 on failure, 0 on success.
*/

int mkdir_recursive(const char *path, mode_t mode);


/** Create needed parent directories of a file or directory.
@param fd The directory path is relative to.
@param path The full path of a file or directory.
@param mode The desired unix mode bits of the parent directories.
@return -1 on failure, 0 on success.
*/

int mkdirat_recursive_parents(int fd, const char *path, mode_t mode);

/** Create needed parent directories of a file or directory.
@param path The full path of a file or directory.
@param mode The desired unix mode bits of the parent directories.
@return -1 on failure, 0 on success.
*/

int mkdir_recursive_parents(const char *path, mode_t mode);

#endif
