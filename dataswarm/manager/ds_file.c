#include "ds_file.h"

#include <stdlib.h>
#include <string.h>

struct ds_file *ds_file_create(struct jx *jfile)
{
	struct ds_file *f = malloc(sizeof(*f));
	memset(f, 0, sizeof(*f));

	f->fileid = jx_lookup_string_dup(jfile, "file-id");
	f->projectid = jx_lookup_string_dup(jfile, "project-id");
	f->size = jx_lookup_integer(jfile, "size");

	f->metadata = jx_lookup(jfile, "metadata");
	if(f->metadata) {
		f->metadata = jx_copy(f->metadata);
	}

	return f;

}

const char *ds_file_state_string(ds_file_state_t state)
{
	switch (state) {
	case DS_FILE_PENDING:
		return "pending";
	case DS_FILE_ALLOCATING:
		return "allocating";
	case DS_FILE_MUTABLE:
		return "mutable";
	case DS_FILE_COMMITTING:
		return "committing";
	case DS_FILE_IMMUTABLE:
		return "immutable";
	case DS_FILE_DELETING:
		return "deleting";
	case DS_FILE_DELETED:
		return "deleted";
	default:
		return "unknown";
	}
}

struct jx *ds_file_to_jx(struct ds_file *f)
{
	struct jx *jfile = jx_object(0);
	if(f->fileid)
		jx_insert_string(jfile, "file-id", f->fileid);
	if(f->projectid)
		jx_insert_string(jfile, "project-id", f->projectid);
	if(f->metadata)
		jx_insert(jfile, jx_string("metadata"), jx_copy(f->metadata));
	if(f->size)
		jx_insert(jfile, jx_string("size"), jx_integer(f->size));
	jx_insert_string(jfile, "state", ds_file_state_string(f->state));

	return jfile;
}


void ds_file_delete(struct ds_file *f)
{
	if(!f)
		return;
	if(f->projectid)
		free(f->projectid);
	if(f->fileid)
		free(f->fileid);
	if(f->metadata)
		jx_delete(f->metadata);
	free(f);
}
