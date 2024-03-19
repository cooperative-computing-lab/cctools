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
	char *name = malloc(2 * sizeof(char));
	struct boxed_int *box;
	name[1] = '\0';

	int total_sum = 0;

	for (i = 0; i < 11; i++) {
		name[0] = 65 + i;
		box = malloc(sizeof(struct boxed_int));
		box->value = i;
		total_sum += i;

		hash_table_insert(h, name, box);
	}

	int start;
	char *key_start = malloc(2 * sizeof(char));
	for (start = 0; start < 11; start++) {
		key_start[0] = 65 + start;

		box = hash_table_lookup(h, key_start);
		if (!box) {
			return 1;
		}

		if (box->value != start) {
			return 1;
		}

		hash_table_fromkey(h, key_start);
		if(!hash_table_nextkey(h, (char **)&name, (void **)&box)) {
			return 1;
		}

		if (box->value == start) {
			fprintf(stdout, "correct value from start %s: %d == %d\n", key_start, start, box->value);
		}
		else {
			fprintf(stdout, "incorrect value from start %s: %d == %d\n", key_start, start, box->value);
			return 1;
		}

		int current_sum = 0;
		int iter_control, iter_count_var;

		HASH_TABLE_ITERATE_FROM_KEY( h, iter_control, iter_count_var, key_start, name, box ) {
			current_sum += box->value;
			fprintf(stdout, "partial sum from %s: %d, added %s %d\n", key_start, current_sum, name, box->value);
		}

		if (current_sum == total_sum) {
			fprintf(stdout, "correct sum from %s: %d == %d\n", key_start, current_sum, total_sum);
		} else {
			fprintf(stdout, "error in sum from %s: %d != %d\n", key_start, current_sum, total_sum);
			return 1;
		}
	}

	hash_table_delete(h);

	return 0;
}
