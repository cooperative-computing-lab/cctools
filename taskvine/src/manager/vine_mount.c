
#include "vine_mount.h"

#include <string.h>
#include <stdlib.h>

struct vine_mount * vine_mount_create( struct vine_file *file, const char *remote_name, vine_file_flags_t flags )
{
	struct vine_mount *m = malloc(sizeof(*m));
	m->file = file;
	if(remote_name) {
		f->remote_name = xxstrdup(remote_name);
	} else {
		f->remote_name = 0;
	}
	m->flags = flags;
	m->substitute = 0;
	return m;
}

void vine_mount_delete( struct vine_mount *m )
{
	if(!m) return 0;
	vine_file_delete(m->file);
	if(m->remote_name) free(m->remote_name);
	free(m);
}
