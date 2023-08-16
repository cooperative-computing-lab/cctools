/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
chirp_audit scans a local path using the chirp_local module,
counting up the files, directories, and bytes consumed by each user.
It returns a hash_table keyed on the user name, with each entry
pointing to a chirp_audit structure.  When done, the caller should
send the hash table back to chirp_audit_delete.
*/

#ifndef CHIRP_AUDIT_H
#define CHIRP_AUDIT_H

#include "int_sizes.h"
#include "hash_table.h"
#include "chirp_client.h"

struct hash_table *chirp_audit(const char *path);
void chirp_audit_delete(struct hash_table *table);

#endif

/* vim: set noexpandtab tabstop=8: */
