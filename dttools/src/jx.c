#include "jx.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

jx_pair_t * jx_pair( jx_t *key, jx_t *value, jx_pair_t *next )
{
	jx_pair_t *pair = malloc(sizeof(*pair));
	pair->key = key;
	pair->value = value;
	pair->next = next;
	return pair;
}

void jx_pair_print( jx_pair_t *pair, FILE *file )
{
	jx_print(pair->key,file);
	fprintf(file," : ");
	jx_print(pair->value,file);
	if(pair->next) {
		fprintf(file,",");
		fprintf(file,"\n");
		jx_pair_print(pair->next,file);
	}
}

jx_pair_t * jx_pair_copy( jx_pair_t *p )
{
	jx_pair_t *pair = malloc(sizeof(*pair));
	pair->key = jx_copy(p->key);
	pair->value = jx_copy(p->value);
	pair->next = jx_pair_copy(p->next);
	return pair;
}

int jx_pair_is_constant( jx_pair_t *p )
{
	return jx_is_constant(p->key)
		&& jx_is_constant(p->value)
		&& jx_pair_is_constant(p->next);
}

jx_pair_t * jx_pair_evaluate( jx_pair_t *pair, jx_eval_func_t func )
{
	return jx_pair(
		jx_evaluate(pair->key,func),
		jx_evaluate(pair->value,func),
		jx_pair_evaluate(pair->next,func)
	);
}

void jx_pair_delete( jx_pair_t *pair )
{
	if(!pair) return;
	jx_delete(pair->key);
	jx_delete(pair->value);
       	jx_pair_delete(pair->next);
	free(pair);
}

jx_item_t * jx_item( jx_t *value, jx_item_t *next )
{
	jx_item_t *item = malloc(sizeof(*item));
	item->value = value;
	item->next = next;
	return item;
}

void jx_item_print( jx_item_t *item, FILE *file )
{
	if(!item) return;
	jx_print(item->value,file);
	if(item->next) {
		fprintf(file,",");
		jx_item_print(item->next,file);
	}
}

jx_item_t * jx_item_evaluate( jx_item_t *item, jx_eval_func_t func )
{
	return jx_item(
		jx_evaluate(item->value,func),
		jx_item_evaluate(item->next,func)
	);
}

int jx_item_is_constant( jx_item_t *i )
{
	return jx_is_constant(i->value) && jx_item_is_constant(i->next);
}

jx_item_t * jx_item_copy( jx_item_t *i )
{
	jx_item_t *item = malloc(sizeof(*item));
	item->value = jx_copy(i->value);
	item->next = jx_item_copy(i->next);
	return item;
}

void jx_item_delete( jx_item_t *item )
{
	if(!item) return;
	jx_delete(item->value);
       	jx_item_delete(item->next);
	free(item);
}

static jx_t * jx_create( jx_type_t type )
{
	jx_t *j = malloc(sizeof(*j));
	j->type = type;
	return j;
}

jx_t * jx_null()
{
	return jx_create(JX_NULL);
}

jx_t * jx_symbol( const char *symbol_name )
{
	jx_t *j = jx_create(JX_SYMBOL);
	j->symbol_name = strdup(symbol_name);
	return j;
}

jx_t * jx_string( const char *string_value )
{
	jx_t *j = jx_create(JX_STRING);
	j->string_value = strdup(string_value);
	return j;
}

jx_t * jx_integer( int integer_value )
{
	jx_t *j = jx_create(JX_INTEGER);
	j->integer_value = integer_value;
	return j;
}

jx_t * jx_float( double float_value )
{
	jx_t *j = jx_create(JX_FLOAT);
	j->float_value = float_value;
	return j;
}

jx_t * jx_boolean( int boolean_value )
{
	jx_t *j = jx_create(JX_BOOLEAN);
	j->boolean_value = boolean_value;
	return j;
}

jx_t * jx_object( jx_pair_t *pairs )
{
	jx_t *j = jx_create(JX_OBJECT);
	j->pairs = pairs;
	return j;
}

jx_t * jx_array( jx_item_t *items )
{
	jx_t *j = jx_create(JX_ARRAY);
	j->items = items;
	return j;
}

jx_t * jx_object_lookup( jx_t *j, const char *key )
{
	jx_pair_t *p;

	if(!j || j->type!=JX_OBJECT) return 0;

	for(p=j->pairs;p;p=p->next) {
		if(p->key && p->key->type==JX_STRING) {
			if(!strcmp(p->key->string_value,key)) {
				return p->value;
			}
		}
	}

	return 0;
}

int jx_object_insert( jx_t *j, jx_t *key, jx_t *value )
{
	if(!j || j->type!=JX_OBJECT) return 0;
	j->pairs = jx_pair(key,value,j->pairs);
	return 1;
}

void jx_delete( jx_t *j )
{
	if(!j) return;

	switch(j->type) {
		case JX_FLOAT:
		case JX_BOOLEAN:
		case JX_INTEGER:
		case JX_NULL:
			break;
		case JX_SYMBOL:
			free(j->symbol_name);
			break;
		case JX_STRING:
			free(j->string_value);
			break;
		case JX_ARRAY:
			jx_item_delete(j->items);
			break;
		case JX_OBJECT:
			jx_pair_delete(j->pairs);
			break;
	}
}

int jx_pair_equals( struct jx_pair *j, struct jx_pair *k ) 
{
	return jx_equals(j->key,k->key) && jx_equals(j->value,k->value) && jx_pair_equals(j->next,k->next);
}

int jx_item_equals( struct jx_item *j, struct jx_item *k ) 
{
	return jx_equals(j->value,k->value) && jx_item_equals(j->next,k->next);
}

int jx_equals( jx_t *j, jx_t *k )
{
	if(!j && !k) return 1;
	if(!j || !k) return 0;
	if(j->type!=k->type) return 0;

	switch(j->type) {
		case JX_NULL:
			return 1;
		case JX_FLOAT:
			return j->float_value==k->float_value;
		case JX_BOOLEAN:
			return j->boolean_value==k->boolean_value;
		case JX_INTEGER:
			return j->integer_value==k->integer_value;
		case JX_SYMBOL:
			return !strcmp(j->symbol_name,k->symbol_name);
		case JX_STRING:
			return !strcmp(j->string_value,k->string_value);
		case JX_ARRAY:
			return jx_item_equals(j->items,k->items);
		case JX_OBJECT:
			return jx_pair_equals(j->pairs,k->pairs);
	}
}

jx_t  *jx_copy( jx_t *j )
{
	switch(j->type) {
		case JX_NULL:
			return jx_null();
		case JX_FLOAT:
			return jx_float(j->float_value);
		case JX_BOOLEAN:
			return jx_boolean(j->boolean_value);
		case JX_INTEGER:
			return jx_integer(j->integer_value);
		case JX_SYMBOL:
			return jx_symbol(j->symbol_name);
		case JX_STRING:
			return jx_string(j->string_value);
		case JX_ARRAY:
			return jx_array(jx_item_copy(j->items));
		case JX_OBJECT:
			return jx_object(jx_pair_copy(j->pairs));
	}
}

void jx_print( jx_t *j, FILE *file )
{
	switch(j->type) {
		case JX_NULL:
			fprintf(file,"null");
			break;
		case JX_FLOAT:
			fprintf(file,"%lg",j->float_value);
			break;
		case JX_BOOLEAN:
			fprintf(file,"%s",j->boolean_value ? "true" : "false");
			break;
		case JX_INTEGER:
			fprintf(file,"%d",j->integer_value);
			break;
		case JX_SYMBOL:
			fprintf(file,"%s",j->symbol_name);
			break;
		case JX_STRING:
			// XXX escape quotes here
			fprintf(file,"\"%s\"",j->string_value);
			break;
		case JX_ARRAY:
			fprintf(file,"[");
			jx_item_print(j->items,file);
			fprintf(file,"]");
			break;
		case JX_OBJECT:
			fprintf(file,"\n{\n");
			jx_pair_print(j->pairs,file);
			fprintf(file,"\n}\n");
			break;
	}
}

int jx_is_constant( struct jx *j )
{
	switch(j->type) {
		case JX_SYMBOL:
			return 0;
		case JX_FLOAT:
		case JX_BOOLEAN:
		case JX_INTEGER:
		case JX_STRING:
		case JX_NULL:
			return 1;
		case JX_ARRAY:
			return jx_item_is_constant(j->items);
		case JX_OBJECT:
			return jx_pair_is_constant(j->pairs);
	}
}

jx_t * jx_evaluate( struct jx *j, jx_eval_func_t func )
{
	switch(j->type) {
		case JX_SYMBOL:
			return func(j->symbol_name);
		case JX_FLOAT:
		case JX_BOOLEAN:
		case JX_INTEGER:
		case JX_STRING:
		case JX_NULL:
			return jx_copy(j);
		case JX_ARRAY:
			return jx_array(jx_item_evaluate(j->items,func));
		case JX_OBJECT:
			return jx_object(jx_pair_evaluate(j->pairs,func));
	}
}

#if 0

typedef enum {
	TOKEN_SYMBOL,
	TOKEN_INTEGER,
	TOKEN_FLOAT,
	TOKEN_STRING,
	TOKEN_LPAREN,
	TOKEN_RPAREN,
	TOKEN_LBRACKET,
	TOKEN_RBRACKET,
	TOKEN_LBRACE,
	TOKEN_RBRACE,
	TOKEN_COMMA,
	TOKEN_SEMI,
	TOKEN_COLON,
	TOKEN_LPAREN,
	TOKEN_RPAREN
} token_t;

struct scanner {
	char text[MAX_TOKEN_SIZE];
	FILE *source;
};

token_t scanner_read( struct scanner *s )
{
		int c = fgetchar(file);

		if(isspace(c)) {
			continue;
		} else if(c=='{') {
			return TOKEN_LBRACE;
		} else if(c=='}') {
			return TOKEN_RBRACE;
		} else if(c=='[') {
			return TOKEN_LBRACKET;
		} else if(c==']') {
			return TOKEN_LBRACKET;
		} else if(c==',') {
			return TOKEN_COMMA;
		} else if(c==':') {
			return TOKEN_COLON;
		} else if(isalpha(c)) {
			s->text[0] = c;
			for(i=1;i<MAX_TOKEN_SIZE;i++) {
				c = fgetchar(file);
				if(isalpha(c)) {	
					s->text[i] = c;
				} else {
					ungetc(c,file);
					s->text[i] = 0;
					if(!strcmp(s->text,"true")) {
						return TOKEN_TRUE;
					} else if(!strcmp(s->text,"false")) {
						return TOKEN_FALSE;
					} else if(!strcmp(s->text,"null")) {
						return TOKEN_NULL;
					} else {
						return TOKEN_ID;
					}
				}
			}
			abort();
		} else if(isdigit(c)) {
			s->text[0] = c;
       			c = fgetchar(file);
	       		while(isdigit(c)) {
				s->text[i] = c;
			}
			s->text[i] = 0;
			ungetchar(c,file);

		} else if(c=='\"') {
			for(i=0;i<MAX_TOKEN_SIZE;i++) {
				c = fgetchar(file);
				if(c=='\"') {
					s->text[i] = 0;
					return TOKEN_STRING;
				} else if(c=='\\') {
					// XXX substitute backwhacks
					c = fgetchar(file);
					s->text[i] = c;
				} else {
					s->text[i] = c;
				}				
			}
			abort();
		}
	}
}

#endif
