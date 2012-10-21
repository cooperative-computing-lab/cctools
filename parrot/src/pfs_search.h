/*
Copyright (C) 2011- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_SEARCH_H
#define PFS_SEARCH_H

#include <stdlib.h>

struct searchent {
	char *path;
	struct stat *info;
	int errsource;
	int err;
};

struct searchstream {
	struct searchent *entry;
	char *data;
	int i;
};

#define SEARCH struct searchstream

#define PFS_SEARCH_DELIMITER   ':'
#define PFS_SEARCH_DEPTH_MAX   200

#define PFS_SEARCH_STOPATFIRST (1<<0)
#define PFS_SEARCH_RECURSIVE   (1<<1)
#define PFS_SEARCH_METADATA    (1<<2)
#define PFS_SEARCH_INCLUDEROOT (1<<3)
#define PFS_SEARCH_PERIOD      (1<<4)
#define PFS_SEARCH_R_OK        (1<<5)
#define PFS_SEARCH_W_OK        (1<<6)
#define PFS_SEARCH_X_OK        (1<<7)

#define PFS_SEARCH_ERR_OPEN    1
#define PFS_SEARCH_ERR_READ    2
#define PFS_SEARCH_ERR_CLOSE   3
#define PFS_SEARCH_ERR_STAT    4

#endif
