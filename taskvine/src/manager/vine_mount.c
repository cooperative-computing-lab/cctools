/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_mount.h"
#include "debug.h"

#include <stdlib.h>
#include <string.h>

#include "xxmalloc.h"

struct vine_mount *vine_mount_create(
		struct vine_file *file, const char *remote_name, vine_mount_flags_t flags, struct vine_file *substitute)
{
	struct vine_mount *m = malloc(sizeof(*m));

	/* Add a reference each time a file is connected. */
	m->file = vine_file_clone(file);

	if (remote_name) {
		m->remote_name = xxstrdup(remote_name);
	} else {
		m->remote_name = 0;
	}
	m->flags = flags;
	m->substitute = substitute;

	return m;
}

void vine_mount_delete(struct vine_mount *m)
{
	if (!m)
		return;
	vine_file_delete(m->file);
	free(m->remote_name);
	free(m);
}

struct vine_mount *vine_mount_copy(struct vine_mount *m)
{
	if (!m)
		return 0;
	return vine_mount_create(vine_file_clone(m->file), m->remote_name, m->flags, vine_file_clone(m->substitute));
}
