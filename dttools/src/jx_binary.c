/*
Copyright (C) 2019- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_binary.h"
#include "jx.h"
#include "debug.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/*
Rather than relying on the enumeration in jx.h, we rely
on a distinct set of values for binary representations,
since it differs slightly (note TRUE/FALSE/END) and must
not change, unlike the in-memory enumeration of jx.h
*/

#define JX_BINARY_NULL 11
#define JX_BINARY_TRUE 12
#define JX_BINARY_FALSE 13
#define JX_BINARY_INTEGER0 14
#define JX_BINARY_INTEGER8 15
#define JX_BINARY_INTEGER16 16
#define JX_BINARY_INTEGER32 17
#define JX_BINARY_INTEGER64 18
#define JX_BINARY_STRING8 19
#define JX_BINARY_STRING16 20
#define JX_BINARY_STRING32 21
#define JX_BINARY_DOUBLE 22
#define JX_BINARY_ARRAY 23
#define JX_BINARY_OBJECT 24
#define JX_BINARY_END 25

static int jx_binary_write_data( FILE *stream, void *data, unsigned length )
{
	return fwrite(data,length,1,stream);
}

static int jx_binary_write_uint8( FILE *stream, uint8_t i )
{
	return jx_binary_write_data(stream,&i,sizeof(i));
}

static int jx_binary_write_uint16( FILE *stream, uint16_t i )
{
	return jx_binary_write_data(stream,&i,sizeof(i));
}

static int jx_binary_write_uint32( FILE *stream, uint32_t i )
{
	return jx_binary_write_data(stream,&i,sizeof(i));
}

static int jx_binary_write_int8( FILE *stream, int8_t i )
{
	return jx_binary_write_data(stream,&i,sizeof(i));
}

static int jx_binary_write_int16( FILE *stream, int16_t i )
{
	return jx_binary_write_data(stream,&i,sizeof(i));
}

static int jx_binary_write_int32( FILE *stream, int32_t i )
{
	return jx_binary_write_data(stream,&i,sizeof(i));
}

static int jx_binary_write_int64( FILE *stream, int64_t i )
{
	return jx_binary_write_data(stream,&i,sizeof(i));
}

static int jx_binary_write_double( FILE *stream, double d )
{
	return jx_binary_write_data(stream,&d,sizeof(d));
}

int jx_binary_write( FILE *stream, struct jx *j )
{
	struct jx_pair *pair;
	struct jx_item *item;
	uint32_t length;
	int64_t i;

	switch(j->type) {
		case JX_NULL:
			jx_binary_write_uint8(stream,JX_BINARY_NULL);
			break;
		case JX_BOOLEAN:
			if(j->u.boolean_value) {
				jx_binary_write_uint8(stream,JX_BINARY_TRUE);
			} else {
				jx_binary_write_uint8(stream,JX_BINARY_FALSE);
			}
			break;
		case JX_INTEGER:
			i = j->u.integer_value;
			if(i==0) {
				jx_binary_write_uint8(stream,JX_BINARY_INTEGER0);
			} else if(i>=-128 && i<128) {
				jx_binary_write_uint8(stream,JX_BINARY_INTEGER8);
				jx_binary_write_int8(stream,i);
			} else if(i>=-32768 && i<32768) {
				jx_binary_write_uint8(stream,JX_BINARY_INTEGER16);
				jx_binary_write_int16(stream,i);
			} else if(i>=-2147483648 && i<2147483648) {
				jx_binary_write_uint8(stream,JX_BINARY_INTEGER32);
				jx_binary_write_int32(stream,i);
			} else {
				jx_binary_write_uint8(stream,JX_BINARY_INTEGER64);
				jx_binary_write_int64(stream,i);
			}
			break;
		case JX_DOUBLE:
			jx_binary_write_uint8(stream,JX_BINARY_DOUBLE);
			jx_binary_write_double(stream,j->u.double_value);
			break;
		case JX_STRING:
			length = strlen(j->u.string_value);
			if(length<256) {
				jx_binary_write_uint8(stream,JX_BINARY_STRING8);
				jx_binary_write_uint8(stream,length);
			} else if(length<65536) {
				jx_binary_write_uint8(stream,JX_BINARY_STRING16);
				jx_binary_write_uint16(stream,length);
			} else {
				jx_binary_write_uint8(stream,JX_BINARY_STRING32);
				jx_binary_write_uint32(stream,length);
			}
			jx_binary_write_data(stream,j->u.string_value,length);
			break;
		case JX_ARRAY:
			jx_binary_write_uint8(stream,JX_BINARY_ARRAY);
			for(item=j->u.items;item;item=item->next) {
				jx_binary_write(stream,item->value);
			}
			jx_binary_write_uint8(stream,JX_BINARY_END);
			break;
		case JX_OBJECT:
			jx_binary_write_uint8(stream,JX_BINARY_OBJECT);
			for(pair=j->u.pairs;pair;pair=pair->next) {
				jx_binary_write(stream,pair->key);
				jx_binary_write(stream,pair->value);
			}
			jx_binary_write_uint8(stream,JX_BINARY_END);
			break;
		case JX_OPERATOR:
		case JX_SYMBOL:
		case JX_ERROR:
			debug(D_NOTICE,"cannot write out non-constant JX data!");
			return 0;
			break;
	}

	return 1;
}

static int jx_binary_read_data( FILE *stream, void *data, unsigned length )
{
	return fread(data,length,1,stream);
}

static int jx_binary_read_uint8( FILE *stream, uint8_t *i )
{
	return jx_binary_read_data(stream,i,sizeof(*i));
}

static int jx_binary_read_uint16( FILE *stream, uint16_t *i )
{
	return jx_binary_read_data(stream,i,sizeof(*i));
}

static int jx_binary_read_uint32( FILE *stream, uint32_t *i )
{
	return jx_binary_read_data(stream,i,sizeof(*i));
}

static int jx_binary_read_int8( FILE *stream, int8_t *i )
{
	return jx_binary_read_data(stream,i,sizeof(*i));
}

static int jx_binary_read_int16( FILE *stream, int16_t *i )
{
	return jx_binary_read_data(stream,i,sizeof(*i));
}

static int jx_binary_read_int32( FILE *stream, int32_t *i )
{
	return jx_binary_read_data(stream,i,sizeof(*i));
}

static int jx_binary_read_int64( FILE *stream, int64_t *i )
{
	return jx_binary_read_data(stream,i,sizeof(*i));
}

static int jx_binary_read_double( FILE *stream, double *d )
{
	return jx_binary_read_data(stream,d,sizeof(*d));
}

static struct jx_pair * jx_binary_read_pair( FILE *stream )
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

static struct jx_item * jx_binary_read_item( FILE *stream )
{
	struct jx *a = jx_binary_read(stream);
	if(!a) return 0;

	return jx_item(a,0);
}

static struct jx * jx_binary_read_string( FILE *stream, uint32_t length )
{
	char *s = malloc(length+1);
	jx_binary_read_data(stream,s,length);
	s[length] = 0;
	return jx_string_nocopy(s);
}

struct jx * jx_binary_read( FILE *stream )
{
	uint8_t type;
	int8_t i8;
	int16_t i16;
	int32_t i32;
	int64_t i64;
	uint8_t u8;
	uint16_t u16;
	uint32_t u32;
	double d;
	struct jx *arr;
	struct jx *obj;
	struct jx_pair **pair;
	struct jx_item **item;
	
	int result = jx_binary_read_uint8(stream,&type);
	if(!result) return 0;

	switch(type) {
		case JX_BINARY_NULL:
			return jx_null();
		case JX_BINARY_TRUE:
			return jx_boolean(1);
		case JX_BINARY_FALSE:
			return jx_boolean(0);
		case JX_BINARY_INTEGER0:
			return jx_integer(0);
		case JX_BINARY_INTEGER8:
			jx_binary_read_int8(stream,&i8);
			return jx_integer(i8);
		case JX_BINARY_INTEGER16:
			jx_binary_read_int16(stream,&i16);
			return jx_integer(i16);
		case JX_BINARY_INTEGER32:
			jx_binary_read_int32(stream,&i32);
			return jx_integer(i32);
		case JX_BINARY_INTEGER64:
			jx_binary_read_int64(stream,&i64);
			return jx_integer(i64);
		case JX_BINARY_DOUBLE:
			jx_binary_read_double(stream,&d);
			return jx_double(d);
		case JX_BINARY_STRING8:
			jx_binary_read_uint8(stream,&u8);
			return jx_binary_read_string(stream,u8);
		case JX_BINARY_STRING16:
			jx_binary_read_uint16(stream,&u16);
			return jx_binary_read_string(stream,u16);
		case JX_BINARY_STRING32:
			jx_binary_read_uint32(stream,&u32);
			return jx_binary_read_string(stream,u32);
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
			return arr;
			break;
		case JX_BINARY_OBJECT:
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
			return obj;
			break;
		case JX_BINARY_END:
			return 0;
		default:
			debug(D_NOTICE,"unexpected type %d in binary JX data",type);
			return 0;
			break;
	}

	return 0;
}


