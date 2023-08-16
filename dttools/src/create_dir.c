/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "create_dir.h"

#include "mkdir_recursive.h"

int create_dir (const char *path, mode_t mode)
{
	return mkdir_recursive(path, mode) == 0 ? 1 : 0;
}

int create_dir_parents (const char *path, mode_t mode)
{
	return mkdir_recursive_parents(path, mode) == 0 ? 1 : 0;
}

/* vim: set noexpandtab tabstop=8: */
