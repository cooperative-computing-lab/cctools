/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "nvpair.h"
#include "jx.h"
#include "jx_print.h"
#include "stringtools.h"

struct nvpair * jx_to_nvpair( struct jx *object )
{
	struct nvpair *nv = nvpair_create();
	struct jx_pair *p;

	for(p=object->pairs;p;p=p->next) {
		if(p->value->type==JX_STRING) {
			nvpair_insert_string(nv,p->key->string_value,p->value->string_value);
		} else {
			char *s = jx_print_string(p->value);
			nvpair_insert_string(nv,p->key->string_value,s);
			free(s);
		}
	}

	return nv;
}

struct jx * nvpair_to_jx( struct nvpair *nv )
{
	struct jx *object = jx_object(0);

	char *key;
	char *value;
	struct jx *jvalue;

	nvpair_first_item(nv);
	while(nvpair_next_item(nv,&key,&value)) {
		if(string_is_integer(value)) {
			jvalue = jx_integer(atoll(value));
		} else if(string_is_float(value)) {
			jvalue = jx_float(atof(value));
		} else {
			jvalue = jx_string(value);
		}
		jx_insert(object,jx_string(key),jvalue);
	}

	return object;
}


