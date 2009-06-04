/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef DELETE_DIR_H
#define DELETE_DIR_H

/** @file delete_dir.h Delete a directory recursively. */

/** Delete a directory recursively.
@param dir The full path of the directory to delete.
@return One on success, zero on failure.
*/

int delete_dir( const char *dir );

#endif
