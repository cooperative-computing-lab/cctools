
#include "jx_binary.h"
#include "jx.h"
#include "debug.h"

#include <stdio.h>
#include <string.h>
#include <malloc.h>

#define JX_BINARY_NULL 101
#define JX_BINARY_TRUE 102
#define JX_BINARY_FALSE 103
#define JX_BINARY_INTEGER 104
#define JX_BINARY_DOUBLE 105
#define JX_BINARY_STRING 106
#define JX_BINARY_ARRAY 107
#define JX_BINARY_OBJECT 108
#define JX_BINARY_END 109

int jx_binary_write_data( FILE *stream, char *data, int length )
{
	return fwrite(data,length,1,stream);
}

int jx_binary_write_int64( FILE *stream, int64_t i )
{
	return fwrite(&i,sizeof(i),1,stream);
}

int jx_binary_write_double( FILE *stream, double d )
{
	return fwrite(&d,sizeof(d),1,stream);
}

int jx_binary_write_byte( FILE *stream, uint8_t data )
{
	return fwrite(&data,1,1,stream);
}

int jx_binary_write_pair( FILE *stream, struct jx_pair *p )
{
	return jx_binary_write(stream,p->key) && jx_binary_write(stream,p->value);
}

int jx_binary_write_item( FILE *stream, struct jx_item *i )
{
	return jx_binary_write(stream,i->value);
}

int jx_binary_write( FILE *stream, struct jx *j )
{
	struct jx_pair *pair;
	struct jx_item *item;
	int length;

	switch(j->type) {
		case JX_NULL:
			jx_binary_write_byte(stream,JX_BINARY_NULL);
			break;
		case JX_BOOLEAN:
			if(j->u.boolean_value) {
				jx_binary_write_byte(stream,JX_BINARY_TRUE);
			} else {
				jx_binary_write_byte(stream,JX_BINARY_FALSE);
			}
			break;
		case JX_INTEGER:
			jx_binary_write_byte(stream,JX_BINARY_INTEGER);
			jx_binary_write_int64(stream,j->u.integer_value);
			break;
		case JX_DOUBLE:
			jx_binary_write_byte(stream,JX_BINARY_DOUBLE);
			jx_binary_write_int64(stream,j->u.double_value);
			break;
		case JX_STRING:
			jx_binary_write_byte(stream,JX_BINARY_STRING);
			length = strlen(j->u.string_value);
			jx_binary_write_int64(stream,length);
			jx_binary_write_data(stream,j->u.string_value,length);
			break;
		case JX_ARRAY:
			jx_binary_write_byte(stream,JX_BINARY_ARRAY);
			for(item=j->u.items;item;item=item->next) {
				jx_binary_write(stream,item->value);
			}
			jx_binary_write_byte(stream,JX_BINARY_END);
			break;
		case JX_OBJECT:
			jx_binary_write_byte(stream,JX_BINARY_OBJECT);
			for(pair=j->u.pairs;pair;pair=pair->next) {
				jx_binary_write(stream,pair->key);
				jx_binary_write(stream,pair->value);
			}
			jx_binary_write_byte(stream,JX_BINARY_END);
			break;
		case JX_OPERATOR:
		case JX_FUNCTION:
		case JX_SYMBOL:
		case JX_ERROR:
			debug(D_NOTICE,"cannot write out non-constant JX data!");
			return 0;
			break;
	}

	return 1;
}

int jx_binary_read_byte( FILE *stream, uint8_t *data )
{
	return fread(data,1,1,stream);
}

int jx_binary_read_data( FILE *stream, char *data, int length )
{
	return fread(data,length,1,stream);
}

int jx_binary_read_int64( FILE *stream, int64_t *i )
{
	return fread(i,sizeof(*i),1,stream);
}

int jx_binary_read_double( FILE *stream, double *d )
{
	return fread(d,sizeof(*d),1,stream);
}

struct jx_pair * jx_binary_read_pair( FILE *stream )
{
	struct jx *a = jx_binary_read(stream);
	if(!a) return 0;

	struct jx *b = jx_binary_read(stream);
	if(!b) {
		jx_delete(a);
		return 0;
	}

	return jx_pair(a,b,0);
}

struct jx_item * jx_binary_read_item( FILE *stream )
{
	struct jx *a = jx_binary_read(stream);
	if(!a) return 0;

	return jx_item(a,0);
}

struct jx * jx_binary_read( FILE *stream )
{
	uint8_t type;
	struct jx *arr;
	struct jx *obj;
	struct jx_pair **pair;
	struct jx_item **item;
	int64_t length;
	int64_t i;
	double d;
	char *s;
	
	int result = jx_binary_read_byte(stream,&type);
	if(!result) return 0;

	switch(type) {
		case JX_BINARY_NULL:
			return jx_null();
			break;
		case JX_BINARY_TRUE:
			return jx_boolean(1);
			break;
		case JX_BINARY_FALSE:
			return jx_boolean(0);
			break;
		case JX_BINARY_INTEGER:
			jx_binary_read_int64(stream,&i);
			return jx_integer(i);
			break;
		case JX_BINARY_DOUBLE:
			jx_binary_read_double(stream,&d);
			return jx_double(d);
			break;
		case JX_BINARY_STRING:
			jx_binary_read_int64(stream,&length);
			// XXX check copy semantics
			s = malloc(length);
			jx_binary_read_data(stream,s,length);
			return jx_string(s);
			break;
		case JX_BINARY_ARRAY:
			arr = jx_array(0);
			item = &arr->u.items;
			while(1) {
				*item = jx_binary_read_item(stream);
				if(*item) {
					item = &(*item)->next;
				} else {
					break;
				}
			}
			break;
		case JX_OBJECT:
			obj = jx_object(0);
			pair = &obj->u.pairs;
			while(1) {
				*pair = jx_binary_read_pair(stream);
				if(*pair) {
					pair = &(*pair)->next;
				} else {
					break;
				}
			}
			break;
		case JX_BINARY_END:
			return 0;
		case JX_OPERATOR:
		case JX_FUNCTION:
		case JX_SYMBOL:
		case JX_ERROR:
			debug(D_NOTICE,"cannot read out non-constant JX data!");
			return 0;
			break;
	}

	return 0;
}


