#ifndef DATASWARM_FILE_H
#define DATASWARM_FILE_H

#include "jx.h"

typedef enum {
    DS_FILE_PENDING,
    DS_FILE_ALLOCATING,
    DS_FILE_MUTABLE,
    DS_FILE_COMMITTING,
    DS_FILE_IMMUTABLE,
    DS_FILE_DELETING,
	DS_FILE_DELETED
} ds_file_state_t;

struct ds_file {
	char *fileid;
	ds_file_state_t state;
    int size;
    char *projectid;
    struct jx *metadata;
};

struct ds_file * ds_file_create( struct jx *jfile );
struct jx * ds_file_to_jx( struct ds_file *file );
const char * ds_file_state_string( ds_file_state_t state );
void ds_file_delete( struct ds_file *f );

#endif
