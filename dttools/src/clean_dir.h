/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CLEAN_DIR_H
#define CLEAN_DIR_H

/** @file clean_dir.h */

/** Recursively remove all entries in a directory tree that match a given pattern.
@param dir The directory to traverse.
@param delete_pattern The pattern of filenames to match and delete.
*/

int clean_dir( const char *dir, const char *delete_pattern );

#endif
