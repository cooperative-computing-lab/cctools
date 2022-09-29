/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_ALLOC_H
#define CHIRP_ALLOC_H

#include "chirp_types.h"
#include "link.h"

#include <sys/types.h>

int    chirp_alloc_init(int64_t size);
void   chirp_alloc_flush(void);
int    chirp_alloc_flush_needed(void);
time_t chirp_alloc_last_flush_time(void);

int64_t chirp_alloc_lsalloc(const char *path, char *alloc_path, int64_t * total, int64_t * inuse);
int64_t chirp_alloc_mkalloc(const char *path, int64_t size, int64_t mode);

int64_t chirp_alloc_realloc(const char *path, int64_t change, int64_t *inuse);
int64_t chirp_alloc_frealloc (int fd, int64_t change, int64_t *current);

int64_t chirp_alloc_statfs(const char *path, struct chirp_statfs *buf);
int64_t chirp_alloc_fstatfs(int fd, struct chirp_statfs *buf);

#endif

/* vim: set noexpandtab tabstop=4: */
