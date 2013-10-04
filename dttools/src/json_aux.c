#include "json.h"
#include "json_aux.h"

#include <assert.h>
#include <string.h>

json_value *jsonA_getname (json_value *object, const char *name, json_type t)
{
    unsigned int i;
    assert(object->type == json_object);
    for (i = 0; i < object->u.object.length; i++) {
        if (strcmp(name, object->u.object.values[i].name) == 0) {
            if (jistype(object->u.object.values[i].value, t)) {
                return object->u.object.values[i].value;
            } else {
                return NULL;
            }
        }
    }
    return NULL;
}

/* vim: set noexpandtab tabstop=4: */
