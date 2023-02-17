/*
Copyright (C) 2023- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_CHECKSUM_H
#define VINE_CHECKSUM_H

#include <sys/types.h>

char *vine_checksum_any( const char *path, ssize_t *totalsize );

#endif
