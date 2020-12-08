#ifndef DATASWARM_FILE_H
#define DATASWARM_FILE_H

#include "jx.h"
#include "hash_table.h"
#include "itable.h"

typedef enum {
	DS_FILE_PENDING,
	DS_FILE_ALLOCATING,
	DS_FILE_MUTABLE,
	DS_FILE_COMMITTING,
	DS_FILE_IMMUTABLE,
	DS_FILE_DELETING,
	DS_FILE_DELETED
} ds_file_state_t;

typedef enum {
	DS_FILE_TYPE_INPUT,
	DS_FILE_TYPE_OUTPUT,
	DS_FILE_TYPE_STDOUT,
	DS_FILE_TYPE_STDERR
} ds_file_type_t;


struct ds_file {
	char *fileid;
	ds_file_type_t type;
	ds_file_state_t state;
	int size;

	char *projectid;
	struct jx *metadata;
	struct itable *blobs; // Map<struct ds_worker_rep* : struct ds_blob_rep *>
};

struct ds_file *ds_file_create(const char *uuid, const char *projectid, jx_int_t size, struct jx *metadata);
struct jx *ds_file_to_jx(struct ds_file *file);
const char *ds_file_state_string(ds_file_state_t state);
void ds_file_delete(struct ds_file *f);

#endif
