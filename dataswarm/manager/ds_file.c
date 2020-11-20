#include "ds_file.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "jx_parse.h"
#include "jx_print.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

struct ds_file *ds_file_create(const char *uuid, const char *projectid, jx_int_t size, struct jx *metadata)
{
	struct ds_file *f = malloc(sizeof(*f));
	memset(f, 0, sizeof(*f));

	f->fileid = xxstrdup(uuid);
	f->projectid = xxstrdup(projectid);
	f->size = size;

	if(metadata) {
		f->metadata = jx_copy(metadata);
	}

	return f;
}

struct ds_file *ds_file_create_from_file( const char *filename )
{
	FILE *file = fopen(filename,"r");
	if(!file) return 0;

	struct jx *j = jx_parse_stream(file);
	if(!j) {
		fclose(file);
		return 0;
	}

	struct ds_file *f = ds_file_create_from_jx(j);
	jx_delete(j);
	fclose(file);
	return f;
}

struct ds_file * ds_file_create_from_jx( struct jx *j )
{
	struct ds_file *f = malloc(sizeof(*f));

	f->fileid = jx_lookup_string_dup(j,"file-id");
	f->projectid = jx_lookup_string_dup(j,"project-id");
	f->metadata = jx_lookup(j,"metadata");
	f->size = jx_lookup_integer(j,"size");
	f->state = jx_lookup_integer(j,"state");

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

	jx_insert_integer(jfile,"state",f->state);

	return jfile;
}

int ds_file_to_file( struct ds_file *f, const char *filename )
{
	FILE *file = fopen(filename,"w");
	if(!file) return 0;

	struct jx *j = ds_file_to_jx(f);
	if(!j) {
		fclose(file);
		return 0;
	}

	jx_print_stream(j,file);
	jx_delete(j);
	fclose(file);

	return 0;
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
