#include "buffer.h"
#include "copy_stream.h"
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
	json_value *val = jsonA_getname_raw(object, name);

	if(!val || !jistype(val, t)) {
		return NULL;
	} else {
		return val;
	}
}

json_value *jsonA_getname_raw (json_value *object, const char *name)
{
	unsigned int i;
	assert(object->type == json_object);
	for (i = 0; i < object->u.object.length; i++) {
		if (strcmp(name, object->u.object.values[i].name) == 0) {
			return object->u.object.values[i].value;
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

json_value *jsonA_parse_file(const char *path) {
	size_t size;
	char *buffer;

	if(copy_file_to_buffer(path, &buffer, &size) < 1)
		return NULL;

	json_value *J = json_parse(buffer, size);
	free(buffer);

	return J;
}

/* vim: set noexpandtab tabstop=8: */
