
#ifndef VINE_CHECKSUM_H
#define VINE_CHECKSUM_H

#include <sys/types.h>

char *vine_checksum_dir( const char *path );
char *vine_checksum_file( const char *path );
char *vine_checksum_symlink( const char *path, ssize_t length );

char *vine_checksum_any( const char *path );

#endif
