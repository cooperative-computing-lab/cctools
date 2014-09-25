#ifndef JSON_AUX_H
#define JSON_AUX_H

#include "buffer.h"
#include "json.h"

#define jistype(o,t) ((o)->type == (t))

json_value *jsonA_getname (json_value *object, const char *name, json_type t);
int jsonA_escapestring(buffer_t *B, const char *str);

extern const char json_type_str[][10];

#endif
