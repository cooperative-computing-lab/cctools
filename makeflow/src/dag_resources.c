/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "category.h"
#include "dag.h"
#include "hash_table.h"

struct category *makeflow_category_lookup_or_create(const struct dag *d, const char *name) {
	struct category *c = category_lookup_or_create(d->categories, name);

	if(!c->mf_variables) {
		c->mf_variables = hash_table_create(0, 0);
	}

	category_specify_allocation_mode(c, d->allocation_mode);

	return c;
}
