/*
Copyright (C) 2009- Michael Albrecht and The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_location.h"
#include "pfs_types.h"

extern "C" {
#include "debug.h"
#include "list.h"
#include "stringtools.h"
}

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

pfs_location::pfs_location()
{
	data = 0;
}

pfs_location::~pfs_location()
{
	if(data) list_delete(data);
}

int pfs_location::append( const char *srcname )
{
	char name[PFS_PATH_MAX];

	/* Clean up the insane names that systems give us */
	strcpy(name,srcname);
	string_chomp(name);

	/* Create data list if it doesn't already exist */
	if(!data) data = list_create();

	return list_push_tail(data, (void*)strdup(name));

}

int pfs_location::retrieve( char* buf, int buf_len )
{
	char *name;

	debug(D_SYSCALL, "retrieving location");
	if(!data || list_size(data)<=0) return 0;

	name = (char*)list_pop_head(data);
	memset(buf, 0, buf_len);
	strncpy(buf, name, buf_len);
	buf[buf_len-1] = 0;
	free(name);
	return strlen(buf);
}

void add_to_loc( const char *name, void *buf)
{
	pfs_location *loc = (pfs_location *)buf;
	loc->append(name);
}

/* vim: set noexpandtab tabstop=8: */
