/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_ALLOC_H
#define CHIRP_ALLOC_H

#include "chirp_types.h"
#include "link.h"

#include <sys/types.h>

int    chirp_alloc_init(INT64_T size);
void   chirp_alloc_flush(void);
int    chirp_alloc_flush_needed(void);
time_t chirp_alloc_last_flush_time(void);

INT64_T chirp_alloc_lsalloc(const char *path, char *alloc_path, INT64_T * total, INT64_T * inuse);
INT64_T chirp_alloc_mkalloc(const char *path, INT64_T size, INT64_T mode);

INT64_T chirp_alloc_realloc(const char *path, INT64_T change, INT64_T *inuse);
INT64_T chirp_alloc_frealloc (int fd, INT64_T change, INT64_T *current);

INT64_T chirp_alloc_statfs(const char *path, struct chirp_statfs *buf);
INT64_T chirp_alloc_fstatfs(int fd, struct chirp_statfs *buf);

#endif

/* vim: set noexpandtab tabstop=8: */
