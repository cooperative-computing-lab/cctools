/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_GROUP_H
#define CHIRP_GROUP_H

#include <limits.h>

extern char chirp_group_base_url[PATH_MAX];
extern int  chirp_group_cache_time;

int chirp_group_lookup(const char *group, const char *subject);

#endif

/* vim: set noexpandtab tabstop=8: */
