/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MD5_H
#define MD5_H

#include <stdint.h>
#include <stdlib.h>

#define md5_init cctools_md5_init
#define md5_update cctools_md5_update
#define md5_final cctools_md5_final
#define md5_buffer cctools_md5_buffer
#define md5_file cctools_md5_file
#define md5_to_string cctools_md5_to_string
#define md5_of_string cctools_md5_of_string

/** @file md5.h
Routines for computing MD5 checksums.
*/

#define MD5_DIGEST_LENGTH 16
#define MD5_DIGEST_LENGTH_HEX (MD5_DIGEST_LENGTH<<1)

typedef struct {
	uint32_t state[4];
	uint32_t count[2];
	uint8_t buffer[64];
} md5_context_t;

void md5_init(md5_context_t * ctx);
void md5_update(md5_context_t * ctx, const void *, size_t);
void md5_final(unsigned char digest[MD5_DIGEST_LENGTH], md5_context_t * ctx);

/** Checksum a memory buffer.
Note that this function produces a digest in binary form
which  must be converted to a human readable form with @ref md5_to_string.
@param buffer Pointer to a memory buffer.
@param length Length of the buffer in bytes.
@param digest Pointer to a buffer to store the digest.
*/
void md5_buffer(const void *buffer, size_t length, unsigned char digest[MD5_DIGEST_LENGTH]);

/** Convert an MD5 digest into a printable string.
@param digest A binary digest returned from @ref md5_file, @ref md5_buffer, or @ref chirp_reli_md5.
@returns A static pointer to a human readable form of the digest.
*/
const char *md5_to_string(unsigned char digest[MD5_DIGEST_LENGTH]);

/* md5_of_string calculates the md5 checksum of string s.
 * @param s: a string pointer
 * return the md5 checksum of s on success, return NULL on failure.
 * The caller should free the returned string.
 */
char *md5_of_string(const char *s);

/** Checksum a local file.
Note that this function produces a digest in binary form
which  must be converted to a human readable form with @ref md5_to_string.
@param filename Path to the file to checksum.
@param digest Pointer to a buffer to store the digest.
@return One on success, zero on failure.
*/
int md5_file(const char *filename, unsigned char digest[MD5_DIGEST_LENGTH]);

#endif
