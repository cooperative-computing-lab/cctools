#ifndef DS_MEASURE_H
#define DS_MEASURE_H

#include <inttypes.h>


/*
Measure the size of a directory recursively and return the
total number of bytes.  On failure, result is negative and
errno is set.
*/

int64_t ds_measure( const char *path );

/*
Measure the size of a directory and also hash its contents
recursively.  On success, returns the total number of bytes
and fills in the string "hash" with an MD5 checksum.
On failure, result is negative and errno is set.
*/

int64_t ds_measure_and_hash( const char *path, char *hash );


#endif
