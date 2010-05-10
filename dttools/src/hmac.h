#ifndef HMAC_H_
#define HMAC_H_
#include "sha1.h"
#include "md5.h"

int hmac( const char* text, int text_len, const char* key, int key_len, unsigned char *digest, int digest_len, int block_size, void (*hash_func)(const char*, int, unsigned char*));

int hmac_md5( const char* text, int text_len, const char* key, int key_len, unsigned char *digest);
int hmac_sha1( const char* text, int text_len, const char* key, int key_len, unsigned char *digest);


#endif

