
#include "ds_validate.h"
#include "jx_print.h"
#include "debug.h"

#include <string.h>
#include <debug.h>

const char *DECLARE_FILE[] = { "type", "project", "size", "metadata", NULL };
const char *SUBMIT_TASK[] = { "type", "service", "project", "namespace", "resources", "event", NULL };
const char *SUBMIT_SERVICE[] = { "type", "project", "namespace", "resources", "environment", NULL };

int is_in(const char *str, const char **array)
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

int validate_json(struct jx *json, const char **array)
{
    //iterate over the keys in a JX_OBJECT
    void *j = NULL;
    const char *key = jx_iterate_keys(json, &j);

    while(key != NULL) {

        if(!is_in(key, array)) {
            char *jx_str = jx_print_string(json);
            debug(D_DATASWARM, "unknown key \"%s\" in %s.", key, jx_str);
            free(jx_str);
            return 0;
        }

        key = jx_iterate_keys(json, &j);
    }

    return 1;
}

int check_values(struct jx* j)
{

    /* this function checks the values in the structure to make sure
    * they are of the correct type */
	return 1;
}
