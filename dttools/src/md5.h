#ifndef MD5_H
#define MD5_H

/** @file md5.h
Routines for computing MD5 checksums.
*/

#include "int_sizes.h"

#define MD5_DIGEST_LENGTH 16

typedef struct {
	UINT32_T state[4];
	UINT32_T count[2];
	unsigned char buffer[64];
} md5_context_t;

void md5_init( md5_context_t *ctx );
void md5_update( md5_context_t *ctx, const unsigned char *, unsigned int );
void md5_final( unsigned char digest[MD5_DIGEST_LENGTH], md5_context_t *ctx );

/** Checksum a memory buffer.
Note that this function produces a digest in binary form
which  must be converted to a human readable form with @ref md5_string.
@param buffer Pointer to a memory buffer.
@param length Length of the buffer in bytes.
@param digest Pointer to a buffer to store the digest.
*/

void md5_buffer( const char *buffer, int length, unsigned char digest[MD5_DIGEST_LENGTH] );

/** Checksum a local file.
Note that this function produces a digest in binary form
which  must be converted to a human readable form with @ref md5_string.
@param filename Path to the file to checksum.
@param digest Pointer to a buffer to store the digest.
@return One on success, zero on failure.
*/

int md5_file( const char *filename, unsigned char digest[MD5_DIGEST_LENGTH] );

/** Convert an MD5 digest into a printable string.
@param digest A binary digest returned from @ref md5_file, @ref md5_buffer, or @ref chirp_reli_md5.
@returns A static pointer to a human readable form of the digest.
*/

const char * md5_string( unsigned char digest[MD5_DIGEST_LENGTH] );

#endif
