/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CREATE_DIR_H
#define CREATE_DIR_H

/** @file create_dir.h Create a new directory recursively. */

#include <sys/stat.h>

/** Create a new directory recursively.
@param path The full path of a directory.  It is not necessary for all components of the path to exist.
@param mode The desired unix mode bits of the directory and parents.
@return One on success, zero on failure.
*/

int create_dir (const char *path, mode_t mode);

/** Create needed parent directories of a file or directory.
@param path The full path of a file or directory.
@param mode The desired unix mode bits of the parent directories.
@return One on success, zero on failure.
*/

int create_dir_parents (const char *path, mode_t mode);

#endif

/* vim: set noexpandtab tabstop=8: */
