#include "buffer.h"
#include "json.h"
#include "json_aux.h"

#include <assert.h>
#include <string.h>

const char json_type_str[][10] = {
    "NONE",
    "OBJECT",
    "ARRAY",
    "INTEGER",
    "DOUBLE",
    "STRING",
    "BOOLEAN",
    "NULL",
};

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

int jsonA_escapestring(buffer_t *B, const char *str)
{
	for (; *str; str++) {\
		switch (*str) {\
			case '/':
				if (buffer_putliteral(B, "\\/") == -1) return -1;
				break;
			case '\\':
				if (buffer_putliteral(B, "\\\\") == -1) return -1;
				break;
			case '\"':
				if (buffer_putliteral(B, "\\\"") == -1) return -1;
				break;
			case '\b':
				if (buffer_putliteral(B, "\\b") == -1) return -1;
				break;
			case '\f':
				if (buffer_putliteral(B, "\\f") == -1) return -1;
				break;
			case '\n':
				if (buffer_putliteral(B, "\\n") == -1) return -1;
				break;
			case '\r':
				if (buffer_putliteral(B, "\\r") == -1) return -1;
				break;
			case '\t':
				if (buffer_putliteral(B, "\\t") == -1) return -1;
				break;
			default:
				if (buffer_putfstring(B, "%c", (int)*str) == -1) return -1;
				break;
		}
	}
	return 0;
}

/* vim: set noexpandtab tabstop=4: */
