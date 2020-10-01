#ifndef DS_HASH_H
#define DS_HASH_H

#include <inttypes.h>

int64_t ds_hash_and_measure( const char *path, char *hash );

int64_t ds_measure( const char *path );

#endif
