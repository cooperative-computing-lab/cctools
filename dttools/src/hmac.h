/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef HMAC_H_
#define HMAC_H_

/** @file hmac.h
Routines for computing Hash-based Message Authentication Codes.
*/

#include "sha1.h"
#include "md5.h"


/* Generate HMAC
@param text 
@param text_len 
@param key 
@param key_len 
@param digest 
@param digest_len 
@param block_size 
@param hash_func 
*/
int hmac( const char* text, int text_len, const char* key, int key_len, unsigned char *digest, int digest_len, int block_size, void (*hash_func)(const char*, int, unsigned char*));

/* Generate HMAC using md5 hash function
*/
int hmac_md5( const char* text, int text_len, const char* key, int key_len, unsigned char *digest);

/* Generate HMAC using sha1 hash function
*/
int hmac_sha1( const char* text, int text_len, const char* key, int key_len, unsigned char *digest);


#endif

