#ifndef JX_H
#define JX_H

#include <stdio.h>

typedef enum {
	JX_NULL,
	JX_SYMBOL,
	JX_STRING,
	JX_INTEGER,
	JX_FLOAT,
	JX_BOOLEAN,
	JX_ARRAY,
	JX_OBJECT
} jx_type_t;

typedef struct jx_pair {
	struct jx      *key;
	struct jx      *value;
	struct jx_pair *next;
} jx_pair_t;

typedef struct jx_item {
	struct jx      *value;
	struct jx_item *next;
} jx_item_t;

typedef struct jx {
	jx_type_t type;
	union {
		char *    symbol_name;
		char *    string_value;
		double    float_value;
		int       integer_value;
		int       boolean_value;
		jx_pair_t *pairs;
		jx_item_t *items;
	};
} jx_t;

jx_pair_t * jx_pair( jx_t *key, jx_t *value, jx_pair_t *next );
jx_pair_t * jx_pair_copy( jx_pair_t *pair );
void        jx_pair_print( jx_pair_t *pair, FILE *file );
void        jx_pair_delete( jx_pair_t *pair );

jx_item_t * jx_item( jx_t *value, jx_item_t *next );
jx_item_t * jx_item_copy( jx_item_t *item );
void        jx_item_print( jx_item_t *item, FILE *file );
void        jx_item_delete( jx_item_t *item );

jx_t * jx_null();
jx_t * jx_symbol( const char *symbol_name );
jx_t * jx_string( const char *string_value );
jx_t * jx_integer( int integer_value );
jx_t * jx_float( double float_value );
jx_t * jx_boolean( int boolean_value );
jx_t * jx_array( jx_item_t *items );
jx_t * jx_object( jx_pair_t *pairs );

void   jx_assert( jx_t *j, jx_type_t type );
int    jx_equals( jx_t *j, jx_t *k );
jx_t * jx_copy( jx_t *j );
void   jx_delete( jx_t *j );
void   jx_print( jx_t *j, FILE *file );

int    jx_object_insert( jx_t *object, jx_t *key, jx_t *value );
jx_t * jx_object_lookup( jx_t *object, const char *key );

void   jx_array_insert( jx_t *array, jx_t *value );
void   jx_array_append( jx_t *array, jx_t *value );

int     jx_is_constant( struct jx *j );

typedef jx_t * (*jx_eval_func_t) ( const char *ident );
jx_t *  jx_evaluate( jx_t *j, jx_eval_func_t evaluator );

#endif
