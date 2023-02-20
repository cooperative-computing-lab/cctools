/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef HMAC_H_
#define HMAC_H_

#include <stdlib.h>

/** @file hmac.h
Routines for computing Hash-based Message Authentication Codes.
*/

#include "sha1.h"
#include "md5.h"


/** Generate HMAC
@param buffer Pointer to a memory buffer.
@param buffer_length Length of the buffer in bytes.
@param key Pointer to a buffer containing the key for hashing.
@param key_length The length of the key in bytes.
@param digest Pointer to a buffer to store the digest.
@param digest_len The length of the digest buffer.
@param block_size The size of the block used by the hash function in bytes.
@param hash_func A function pointer to the hash function to be used.
*/
int hmac(const void *buffer, size_t buffer_length, const void *key, size_t key_length, unsigned char *digest, size_t digest_len, size_t block_size, void (*hash_func) (const void *, size_t, unsigned char *));

/** Generate HMAC using md5 hash function
Note that this function produces a digest in binary form which must be converted to a human readable form with md5_to_string.
@param buffer Pointer to a memory buffer.
@param buffer_length Length of the buffer in bytes.
@param key Pointer to a buffer containing the key for hashing.
@param key_length The length of the key in bytes.
@param digest Pointer to a buffer of size MD5_DIGEST_LENGTH to store the digest.
*/
int hmac_md5(const void *buffer, size_t buffer_length, const void *key, size_t key_length, unsigned char digest[MD5_DIGEST_LENGTH]);

/** Generate HMAC using sha1 hash function
Note that this function produces a digest in binary form which must be converted to a human readable form with sha1_string.
@param buffer Pointer to a memory buffer.
@param buffer_length Length of the buffer in bytes.
@param key Pointer to a buffer containing the key for hashing.
@param key_length The length of the key in bytes.
@param digest Pointer to a buffer of size SHA1_DIGEST_LENGTH to store the digest.
*/
int hmac_sha1(const void *buffer, size_t buffer_length, const void *key, size_t key_length, unsigned char digest[SHA1_DIGEST_LENGTH]);


#endif
