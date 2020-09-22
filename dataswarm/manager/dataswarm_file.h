#ifndef DATASWARM_FILE_H
#define DATASWARM_FILE_H

#include "jx.h"

typedef enum {
    DATASWARM_FILE_PENDING,
    DATASWARM_FILE_ALLOCATING,
    DATASWARM_FILE_MUTABLE,
    DATASWARM_FILE_COMMITTING,
    DATASWARM_FILE_IMMUTABLE,
    DATASWARM_FILE_DELETING,
	DATASWARM_FILE_DELETED
} dataswarm_file_state_t;

struct dataswarm_file {
	char *fileid;
	dataswarm_file_state_t state;
    int size;
    char *projectid;
    struct jx *metadata;
};

#endif
