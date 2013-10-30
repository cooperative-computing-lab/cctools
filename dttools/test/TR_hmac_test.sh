#!/bin/sh

. ../../dttools/src/test_runner.common.sh

exe="hmac_test.test"

prepare()
{
	gcc -g -o "$exe" -I ../src/ -x c - -x none ../src/libdttools.a -lm <<EOF
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hmac.h"
#include "md5.h"

int main(int argc, char **argv)
{
	unsigned char md5_digest[MD5_DIGEST_LENGTH];
	unsigned char sha1_digest[SHA1_DIGEST_LENGTH];
	char md5_ref1[] = "9294727a3638bb1c13f48ef8158bfc9d";
	char md5_ref2[] = "750c783e6ab0b503eaa86e310a5db738";
	char md5_ref3[] = "56be34521d144c88dbb8c733f0e8b3f6";
	char sha1_ref1[] = "b617318655057264e28bc0b6fb378c8ef146be00";
	char sha1_ref2[] = "effcdf6ae5eb2fa2d27416d5f184df9c259a7c79";
	char sha1_ref3[] = "125d7342b9ac11cd91a39af48aa17b4f63f175d3";

	char data[51];
	const char *string;
	int verbose = 0;

	if(argc > 1 && !strcmp(argv[1], "-v"))
		verbose = 1;

	if(verbose)
		printf("MD5 Tests\n===========\n");

	// Test Str 1
	hmac_md5("Hi There", 8, "\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b", 16, md5_digest);
	string = md5_string(md5_digest);
	if(verbose) {
		printf("MD5 Test 1 ref: \t0x%s\n", md5_ref1);
		printf("MD5 Test 1 digest:\t0x%s\n", string);
	}
	if(strcmp(md5_ref1, string)) {
		if(verbose)
			printf("MD5 Test 1 Failed\n");
		return -1;
	}

	hmac_md5("what do ya want for nothing?", 28, "Jefe", 4, md5_digest);
	string = md5_string(md5_digest);
	if(verbose) {
		printf("\n");
		printf("MD5 Test 2 ref: \t0x%s\n", md5_ref2);
		printf("MD5 Test 2 digest:\t0x%s\n", string);
	}
	if(strcmp(md5_ref2, string)) {
		if(verbose)
			printf("MD5 Test 2 Failed\n");
		return -1;
	}

	memset(data, '\xDD', 50);
	data[50] = 0;
	hmac_md5(data, 50, "\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA", 16, md5_digest);
	string = md5_string(md5_digest);
	if(verbose) {
		printf("\n");
		printf("MD5 Test 3 ref: \t0x%s\n", md5_ref3);
		printf("MD5 Test 3 digest:\t0x%s\n", string);
	}
	if(strcmp(md5_ref3, string)) {
		if(verbose)
			printf("MD5 Test 3 Failed\n");
		return -1;
	}

	if(verbose)
		printf("\n\nSHA1 Tests\n===========\n");

	// Test Str 1
	hmac_sha1("Hi There", 8, "\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b\x0b", 20, sha1_digest);
	string = sha1_string(sha1_digest);
	if(verbose) {
		printf("SHA1 Test 1 ref: \t0x%s\n", sha1_ref1);
		printf("SHA1 Test 1 digest:\t0x%s\n", string);
	}
	if(strcmp(sha1_ref1, string)) {
		if(verbose)
			printf("SHA1 Test 1 Failed\n");
		return -1;
	}

	hmac_sha1("what do ya want for nothing?", 28, "Jefe", 4, sha1_digest);
	string = sha1_string(sha1_digest);
	if(verbose) {
		printf("\n");
		printf("SHA1 Test 2 ref: \t0x%s\n", sha1_ref2);
		printf("SHA1 Test 2 digest:\t0x%s\n", string);
	}
	if(strcmp(sha1_ref2, string)) {
		if(verbose)
			printf("SHA1 Test 2 Failed\n");
		return -1;
	}

	memset(data, '\xDD', 50);
	data[50] = 0;
	hmac_sha1(data, 50, "\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA\xAA", 20, sha1_digest);
	string = sha1_string(sha1_digest);
	if(verbose) {
		printf("\n");
		printf("SHA1 Test 3 ref: \t0x%s\n", sha1_ref3);
		printf("SHA1 Test 3 digest:\t0x%s\n", string);
	}
	if(strcmp(sha1_ref3, string)) {
		if(verbose)
			printf("SHA1 Test 3 Failed\n");
		return -1;
	}

	return 0;
}
EOF
	return $?
}

run()
{
	./"$exe" -v
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch $@

# vim: set noexpandtab tabstop=4:
