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

struct jx_pair {
	struct jx      *key;
	struct jx      *value;
	struct jx_pair *next;
};

struct jx_item {
	struct jx      *value;
	struct jx_item *next;
};

struct jx {
	jx_type_t type;
	union {
		char *    symbol_name;
		char *    string_value;
		double    float_value;
		int       integer_value;
		int       boolean_value;
		struct jx_pair *pairs;
		struct jx_item *items;
	};
};

struct jx_pair * jx_pair( struct jx *key, struct jx *value, struct jx_pair *next );
struct jx_pair * jx_pair_copy( struct jx_pair *pair );
void             jx_pair_delete( struct jx_pair *pair );

struct jx_item * jx_item( struct jx *value, struct jx_item *next );
struct jx_item * jx_item_copy( struct jx_item *item );
void             jx_item_delete( struct jx_item *item );

struct jx * jx_null();
struct jx * jx_symbol( const char *symbol_name );
struct jx * jx_string( const char *string_value );
struct jx * jx_integer( int integer_value );
struct jx * jx_float( double float_value );
struct jx * jx_boolean( int boolean_value );
struct jx * jx_array( struct jx_item *items );
struct jx * jx_object( struct jx_pair *pairs );

void        jx_assert( struct jx *j, jx_type_t type );
int         jx_equals( struct jx *j, struct jx *k );
struct jx * jx_copy( struct jx *j );
void        jx_delete( struct jx *j );

int         jx_object_insert( struct jx *object, struct jx *key, struct jx *value );
struct jx * jx_object_lookup( struct jx *object, const char *key );

void        jx_array_insert( struct jx *array, struct jx *value );
void        jx_array_append( struct jx *array, struct jx *value );

int     jx_is_constant( struct jx *j );

typedef struct jx * (*jx_eval_func_t) ( const char *ident );
struct jx *  jx_evaluate( struct jx *j, jx_eval_func_t evaluator );

#endif
