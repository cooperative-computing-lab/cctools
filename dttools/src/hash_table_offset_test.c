/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "hash_table.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

struct boxed_int {
	int value;
};

int main(int argc, char **argv) {

	struct hash_table *h = hash_table_create(0, 0);

	char i;
	char name[2];
	name[1] = '\0';

	for (i = 0; i < 11; i++) {
		name[0] = 65 + i;
		struct boxed_int *box = malloc(sizeof(struct boxed_int));
		box->value = i;
		hash_table_insert(h, name, box);
	}

	for (i = 0; i < 127; i++) {
		int offset;
		char *key;
		struct boxed_int *box;
		int sum = 0;
		HASH_TABLE_ITERATE_RANDOM_START(h, offset, key, box) {
			sum += box->value;
		}

		if(sum != 55) {
			return 1;
		}
	}

	char **keys = hash_table_keys_array(h);
	char *key;
	int j = 0;
	int sum = 0;

	while ((key = keys[j])) {
		struct boxed_int *box = hash_table_lookup(h, key);
		sum += box->value;
		j++;
	}

	hash_table_free_keys_array(keys);

	if(sum != 55) {
		return 1;
	}

	hash_table_clear(h, free);
	hash_table_delete(h);

	return 0;
}
