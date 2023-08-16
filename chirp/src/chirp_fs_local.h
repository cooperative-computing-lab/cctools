/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_LOCAL_H
#define CHIRP_LOCAL_H

#include "chirp_filesystem.h"

extern struct chirp_filesystem chirp_fs_local;

int chirp_fs_local_resolve (const char *path, int *dirfd, char basename[CHIRP_PATH_MAX], int follow);

#endif

/* vim: set noexpandtab tabstop=8: */
