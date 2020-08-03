
#include "helpers.h"

#include <string.h>

static const char *declaration[] = { "type", "project", "metadata" }

static int is_in(const char *str, const char **array)
{

	const char **ptr = array;

	while(*ptr != 0) {

		if(!strcmp(*ptr, str)) {
			return 1;
		}

		ptr++;

	}

	return 0;

}

static int validate_json(struct jx *json, const char **array)
{

	//iterate over the keys in a JX_OBJECT
	void *j = NULL;
	const char *key = jx_iterate_keys(json, &j);

	while(key != NULL) {

		if(!is_in(key, array)) {
			return 1;
		}

		key = jx_iterate_keys(json, &j);

	}

	return 0;

}

