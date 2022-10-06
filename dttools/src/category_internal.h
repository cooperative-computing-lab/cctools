/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CCTOOLS_CATEGORY_INT_H
#define CCTOOLS_CATEGORY_INT_H

#include "category.h"

void category_first_allocation_accum_times(struct histogram *h, double *keys, double *tau_mean, double *counts_cdp, double *times_accum);
void category_tune_bucket_size(const char *resource, int64_t size);
int64_t category_get_bucket_size(const char *resource);

#endif
