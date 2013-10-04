#ifndef JSON_AUX_H
#define JSON_AUX_H

#include "json.h"

#define jistype(o,t) ((o)->type == (t))

json_value *jsonA_getname (json_value *object, const char *name, json_type t);

#endif
