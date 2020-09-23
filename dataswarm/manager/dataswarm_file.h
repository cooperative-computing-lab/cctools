#ifndef DS_FILE_H
#define DS_FILE_H

#include "jx.h"

typedef enum {
    DS_FILE_PENDING,
    DS_FILE_ALLOCATING,
    DS_FILE_MUTABLE,
    DS_FILE_COMMITTING,
    DS_FILE_IMMUTABLE,
    DS_FILE_DELETING,
	DS_FILE_DELETED
} dataswarm_file_state_t;

struct dataswarm_file {
	char *fileid;
	dataswarm_file_state_t state;
    int size;
    char *projectid;
    struct jx *metadata;
};

#endif
