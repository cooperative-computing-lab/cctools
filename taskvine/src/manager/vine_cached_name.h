/*
Copyright (C) 2023- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_CACHED_NAME_H
#define VINE_CACHED_NAME_H

#include "vine_file.h"
#include <sys/types.h>

char *vine_cached_name( const struct vine_file *f, ssize_t *totalsize );
char *vine_random_name( const struct vine_file *f, ssize_t *totalsize );

#endif
