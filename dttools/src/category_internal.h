/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CCTOOLS_CATEGORY_INT_H
#define CCTOOLS_CATEGORY_INT_H

#include "category.h"

void category_first_allocation_accum_times(struct itable *histogram, double *tau_mean, int64_t *keys, int64_t *counts_accum, double *times_mean, double *times_accum);
void category_tune_bucket_size(const char *resource, uint64_t size);

#endif
