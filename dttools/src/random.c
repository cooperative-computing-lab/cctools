/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#include "full_io.h"
#include "random.h"
#include "twister.h"
#include "debug.h"

#include <fcntl.h>
#include <unistd.h>

#include <assert.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

void random_init (void)
{
	static int random_initialized = 0;
	if (random_initialized) return;
	int fd = open("/dev/urandom", O_RDONLY);
	if (fd == -1) {
		fd = open("/dev/random", O_RDONLY);
	}
	if (fd >= 0) {
		uint64_t seed[8];
		if (full_read(fd, seed, sizeof(seed)) < (int64_t)sizeof(seed))
			goto nasty_fallback;
		srand(seed[0]);
		twister_init_by_array64(seed, sizeof(seed)/sizeof(seed[0]));
	} else {
		uint64_t seed;
nasty_fallback:
		debug(D_NOTICE, "warning: falling back to low-quality entropy");
		seed = (uint64_t)getpid() ^ (uint64_t)time(NULL);
		seed |= (((uint64_t)(uintptr_t)&seed) << 32); /* using ASLR */
		srand(seed);
		twister_init_genrand64(seed);
	}
	close(fd);
	random_initialized = 1;
	return;
}

int64_t random_int64 (void)
{
	return twister_genrand64_int64();
}

double random_double (void)
{
	return twister_genrand64_real3();
}

void random_array (void *dest, size_t len)
{
	size_t i;
	uint8_t *out = dest;
	for (i = 0; i < len; i += sizeof(int64_t)) {
		int64_t r = twister_genrand64_int64();
		size_t count = len > sizeof(int64_t) ? sizeof(int64_t) : len;
		memcpy(out+i, &r, count);
	}
}

void random_hex (char *str, size_t len)
{
	int64_t r;
	size_t i = 0;
	do {
		r = twister_genrand64_int64();
		snprintf(str+i, len-i, "%016" PRIx64, r);
		i += sizeof(r)*2;
	} while (i < len);
}

/* vim: set noexpandtab tabstop=8: */
