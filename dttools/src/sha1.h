/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef SHA1_H
#define SHA1_H

/** @file sha1.h
Routines for computing SHA1 checksums.
*/

#include "int_sizes.h"

#define SHA1_DIGEST_LENGTH 20
#define SHA1_DIGEST_ASCII_LENGTH 42

typedef struct {
	UINT32_T digest[5];
	UINT32_T countLo, countHi;
	UINT32_T data[16];
	int Endianness;
} sha1_context_t;

void sha1_init( sha1_context_t *ctx );
void sha1_update( sha1_context_t *ctx, const unsigned char *, unsigned int );
void sha1_final( unsigned char digest[SHA1_DIGEST_LENGTH], sha1_context_t *ctx );

/** Checksum a memory buffer.
Note that this function produces a digest in binary form
which  must be converted to a human readable form with @ref sha1_string.
@param buffer Pointer to a memory buffer.
@param length Length of the buffer in bytes.
@param digest Pointer to a buffer to store the digest.
*/

void sha1_buffer( const char *buffer, int length, unsigned char digest[SHA1_DIGEST_LENGTH] );

/** Checksum a local file.
Note that this function produces a digest in binary form
which  must be converted to a human readable form with @ref sha1_string.
@param filename Path to the file to checksum.
@param digest Pointer to a buffer to store the digest.
@return One on success, zero on failure.
*/

int sha1_file( const char *filename, unsigned char digest[SHA1_DIGEST_LENGTH] );

/** Convert an SHA1 digest into a printable string.
@param digest A binary digest returned from @ref sha1_file.
@returns A static pointer to a human readable form of the digest.
*/

const char * sha1_string( unsigned char digest[SHA1_DIGEST_LENGTH] );

#endif
