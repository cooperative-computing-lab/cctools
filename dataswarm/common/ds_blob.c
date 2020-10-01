
#include "ds_blob.h"

#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "debug.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

struct ds_blob * ds_blob_create( const char *blobid, jx_int_t size, struct jx *meta)
{
	struct ds_blob *b = malloc(sizeof(*b));
	memset(b,0,sizeof(*b));
	b->blobid = strdup(blobid);
	b->state = DS_BLOB_RW;
	b->size = size;
	b->md5hash = 0;
	b->meta = jx_copy(meta);
	return b;
}

void ds_blob_delete( struct ds_blob *b )
{
	if(!b) return;
	if(b->meta) jx_delete(b->meta);
	if(b->blobid) free(b->blobid);
	if(b->md5hash) free(b->md5hash);
	free(b);
}

struct ds_blob * ds_blob_create_from_jx( struct jx *jblob )
{
	struct ds_blob *b = malloc(sizeof(*b));
	memset(b,0,sizeof(*b));
	b->blobid = jx_lookup_string_dup(jblob,"blobid");
	b->state = jx_lookup_integer(jblob,"state");
	b->size = jx_lookup_integer(jblob,"size");
	b->md5hash = jx_lookup_string_dup(jblob,"md5hash");
	b->meta = jx_lookup(jblob,"meta");
	if(b->meta) b->meta = jx_copy(b->meta);
	return b;
}

struct ds_blob * ds_blob_create_from_file( const char *filename )
{
	FILE *file = fopen(filename,"r");
	if(!file) return 0;

	struct jx *jblob = jx_parse_stream(file);
	if(!jblob) {
		fclose(file);
		return 0;
	}

	struct ds_blob *b = ds_blob_create_from_jx(jblob);

	jx_delete(jblob);
	fclose(file);

	return b;
}


struct jx * ds_blob_to_jx( struct ds_blob *b )
{
	struct jx *jblob = jx_object(0);
	jx_insert_string(jblob,"blobid",b->blobid);
	jx_insert_integer(jblob,"state",b->state);
	jx_insert_integer(jblob,"size",b->size);
	if(b->md5hash) jx_insert_string(jblob,"md5hash",b->md5hash);
	if(b->meta) jx_insert(jblob,jx_string("meta"),jx_copy(b->meta));
	return jblob;
}

int ds_blob_to_file( struct ds_blob *b, const char *filename )
{
	struct jx *jblob = ds_blob_to_jx(b);
	if(!jblob) return 0;

	FILE *file = fopen(filename,"w");
	if(!file) {
		jx_delete(jblob);
		return 0;
	}

	jx_print_stream(jblob,file);

	jx_delete(jblob);
	fclose(file);

	return 1;
}

