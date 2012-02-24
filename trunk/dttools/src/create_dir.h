/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CREATE_DIR_H
#define CREATE_DIR_H

/** @file create_dir.h Create a new directory recursively. */

/** Create a new directory recursively.
@param path The full path of a directory.  It is not necessary for all components of the path to exist.
@param mode The desired unix mode bits of the directory.
@return One on success, zero on failure.
*/

int create_dir(const char *path, int mode);

#endif
