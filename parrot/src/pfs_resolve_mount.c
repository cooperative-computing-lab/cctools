/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "parrot_client.h"

int pfs_resolve_mount ( const char *path, const char *destination, const char *mode ) {
	return parrot_mount(path, destination, mode);
}

/* vim: set noexpandtab tabstop=4: */
