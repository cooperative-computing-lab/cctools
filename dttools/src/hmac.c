/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <string.h>
#include <stdlib.h>
#include "md5.h"
#include "sha1.h"
#include "hmac.h"


#define MD5_BLOCK_SIZE 64
#define SHA1_BLOCK_SIZE 64

int hmac(const void *text, size_t text_len, const void *in_key, size_t in_key_len, unsigned char *digest, size_t digest_len, size_t block_size, void (*hash_func) (const void *, size_t, unsigned char *))
{
	unsigned char *key;
	char *inner, *outer;
	size_t i;

	key = malloc(block_size);
	inner = malloc(block_size + text_len + 1);
	outer = malloc(block_size + digest_len + 1);
	if(!(key && inner && outer)) {
		if(key)
			free(key);
		if(inner)
			free(inner);
		if(outer)
			free(outer);
		return -1;
	}

	memset(key, 0, block_size);
	memset(inner, 0, block_size + text_len + 1);
	memset(outer, 0, block_size + digest_len + 1);


	if(in_key_len <= block_size) {
		memcpy(key, in_key, in_key_len);
	} else {
		(*hash_func) (in_key, in_key_len, key);
	}

	for(i = 0; i < block_size; i++) {
		inner[i] = key[i] ^ 0x36;	// ipad
		outer[i] = key[i] ^ 0x5c;	// opad
	}

	// INNER HASH
	memcpy(inner + block_size, text, text_len);
	(*hash_func) (inner, block_size + text_len, digest);

	// OUTER HASH
	memcpy(outer + block_size, digest, digest_len);
	(*hash_func) (outer, block_size + digest_len, digest);

	free(key);
	free(inner);
	free(outer);
	return 0;
}

int hmac_md5(const void *text, size_t text_len, const void *in_key, size_t in_key_len, unsigned char digest[MD5_DIGEST_LENGTH])
{
	return hmac(text, text_len, in_key, in_key_len, digest, MD5_DIGEST_LENGTH, MD5_BLOCK_SIZE, &md5_buffer);
}

int hmac_sha1(const void *text, size_t text_len, const void *in_key, size_t in_key_len, unsigned char digest[SHA1_DIGEST_LENGTH])
{
	return hmac(text, text_len, in_key, in_key_len, digest, SHA1_DIGEST_LENGTH, SHA1_BLOCK_SIZE, &sha1_buffer);
}

/* vim: set noexpandtab tabstop=8: */
