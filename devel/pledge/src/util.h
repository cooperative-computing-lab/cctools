#ifndef PL_UTIL_H
#define PL_UTIL_H

#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/param.h>
#include <unistd.h>

char *
rel2abspath(char *abs_p,
		char *rel_p,
		size_t size);
#endif
