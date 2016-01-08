/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_H
#define JX_H

/** @file jx.h JSON Expressions (JX) library.
This module implements extended JSON expressions as a C library.
We use our own custom library to avoid external dependencies and
to implement various extensions to strict JSON.

For example, the following bit of code:

<pre>
struct jx * obj = jx_object(
	jx_pair(
		jx_string("hello"),
		jx_string("world"),
		0)
	);

jx_print_file(jx,stdout);

jx_delete(jx);
</pre>

Will create the following output:
<pre>
{ "hello" : "world" }
</pre>

@see jx_parse.h
@see jx_print.h
*/

#include <stdint.h>
#include <assert.h>

/** JX atomic type.  */

typedef enum {
	JX_NULL,	/**< null value */
	JX_BOOLEAN,	/**< true or false */
	JX_INTEGER,	/**< integer value */
	JX_DOUBLE,	/**< floating point value */
	JX_STRING,	/**< string value */
	JX_SYMBOL,	/**< variable identifier */
	JX_ARRAY,	/**< array containing values */
	JX_OBJECT,	/**< object containing key-value pairs */
} jx_type_t;

typedef int64_t jx_int_t;

/** JX item linked-list used by @ref JX_ARRAY and @ref jx.items */

struct jx_item {
	struct jx      *value;	/**< value of this item */
	struct jx_item *next;	/**< pointer to next item */
};

/** JX key-value pairs used by @ref JX_OBJECT and @ref jx.pairs */

struct jx_pair {
	struct jx      *key;	/**< key of this pair */
	struct jx      *value;  /**< value of this pair */
	struct jx_pair *next;   /**< pointer to next pair */
};

/** JX value representing any expression type. */

struct jx {
	jx_type_t type;               /**< type of this value */
	union {
		int boolean_value;      /**< value of @ref JX_BOOLEAN */
		jx_int_t integer_value; /**< value of @ref JX_INTEGER */
		double double_value;   /**< value of @ref JX_DOUBLE */
		char * string_value;  /**< value of @ref JX_STRING */
		char * symbol_name;   /**< value of @ref JX_SYMBOL */
		struct jx_item *items;  /**< value of @ref JX_ARRAY */
		struct jx_pair *pairs;  /**< value of @ref JX_OBJECT */
	} u;
};

/** Create a JX null value. @return A JX expression. */
struct jx * jx_null();

/** Create a JX boolean value.  @param boolean_value A C boolean value.  @return A JX boolean value.*/
struct jx * jx_boolean( int boolean_value );

/** Create a JX integer value. @param integer_value A C integer. @return a JX integer value. */
struct jx * jx_integer( jx_int_t integer_value );

/** Create a JX floating point value. @param double_value A C double precision floating point.  @return a JX double value. */
struct jx * jx_double( double double_value );

/** Create a JX string value. @param string_value A C string, which will be duplciated via strdup(). @return A JX string value. */
struct jx * jx_string( const char *string_value );

/** Create a JX string value using prinf style formatting.  @param fmt A printf-style format string, followed by matching arguments.  @return A JX string value. */
struct jx * jx_format( const char *fmt, ... );

/** Create a JX symbol. Note that symbols are an extension to the JSON standard. A symbol is a reference to an external variable, which can be resolved by using @ref jx_evaluate. @param symbol_name A C string. @return A JX expression.
*/
struct jx * jx_symbol( const char *symbol_name );

/** Create a JX array.  @param items A linked list of @ref jx_item values.  @return A JX array. */
struct jx * jx_array( struct jx_item *items );

/** Create a JX array with inline items.  @param item One or more items of the array must be given, terminated with a null value.  @return A JX array. */
struct jx * jx_arrayv( struct jx *value, ... );

/** Create a JX object.  @param pairs A linked list of @ref jx_pair key-value pairs.  @return a JX object. */
struct jx * jx_object( struct jx_pair *pairs );

/** Create a JX key-value pair.  @param key The key. @param value The value. @param next The next item in the linked list. @return A key-value pair.*/
struct jx_pair * jx_pair( struct jx *key, struct jx *value, struct jx_pair *next );

/** Create a JX array item.  @param value The value of this item.  @param next The next item in the linked list.  @return An array item. */
struct jx_item * jx_item( struct jx *value, struct jx_item *next );

/** Test an expression's type.  @param j An expression. @param type The desired type. @return True if the expression type matches, false otherwise. */
int jx_istype( struct jx *j, jx_type_t type );

/** Test two expressions for equality. @param j A constant expression. @param k A constant expression. @return True if equal, false if not.
*/
int jx_equals( struct jx *j, struct jx *k );

/** Duplicate an expression. @param j An expression. @return A copy of the expression, which must be deleted by @ref jx_delete
*/
struct jx * jx_copy( struct jx *j );

/** Delete an expression recursively. @param j An expression to delete. */
void jx_delete( struct jx *j );

/** Delete a key-value pair.  @param p The key-value pair to delete. */
void jx_pair_delete( struct jx_pair *p );

/** Delete an array item.  @param i The array item to delete. */
void jx_item_delete( struct jx_item *i );

/** Remove a key-value pair from an object.  @param object The object.  @param key The key. @return The corresponding value, or null if it is not present. */
struct jx * jx_remove( struct jx *object, struct jx *key );

/** Insert a key-value pair into an object.  @param object The object.  @param key The key.  @param value The value. @return True on success, false on failure.  Failure can only occur if the object is not a @ref JX_OBJECT. */
int jx_insert( struct jx *object, struct jx *key, struct jx *value );

/** Insert an integer value into an object @param object The object @param key The key represented as a C string  @param value The integer value. */
void jx_insert_integer( struct jx *object, const char *key, jx_int_t value );

/** Insert a double value into an object @param object The object @param key The key represented as a C string  @param value The double value. */
void jx_insert_double( struct jx *object, const char *key, double value );

/** Insert a string value into an object @param object The object @param key The key represented as a C string  @param value The C string value. */
void jx_insert_string( struct jx *object, const char *key, const char *value );

/** Search for a arbitrary item in an object.  The key is an ordinary string value.  @param object The object in which to search.  @param key The string key to match.  @return The value of the matching pair, or null if none is found. */
struct jx * jx_lookup( struct jx *object, const char *key );

/** Search for a string item in an object.  The key is an ordinary string value.  @param object The object in which to search.  @param key The string key to match.  @return The C string value of the matching object, or null if it is not found, or is not a string. */
const char * jx_lookup_string( struct jx *object, const char *key );

/** Search for an integer item in an object.  The key is an ordinary string value.  @param object The object in which to search.  @param key The string key to match.  @return The integer value of the matching object, or zero if it is not found, or is not an integer. */
jx_int_t jx_lookup_integer( struct jx *object, const char *key );

/** Search for a double item in an object.  The key is an ordinary string value.  @param object The object in which to search.  @param key The string key to match.  @return The double value of the matching object, or null if it is not found, or is not a double. */
double jx_lookup_double( struct jx *object, const char *key );

/** Insert an item at the beginning of an array.  @param array The array to modify.  @param value The value to insert. */
void jx_array_insert( struct jx *array, struct jx *value );

/** Append an item at the end of an array.  @param array The array to modify.  @param value The value to append. */
void jx_array_append( struct jx *array, struct jx *value );

/** Determine if an expression is constant.  Traverses the expression recursively, and returns true if it consists only of constant values, arrays, and objects. @param j The expression to evaluate.  @return True if constant. */
int jx_is_constant( struct jx *j );

/** Export a jx object as a set of environment variables.  @param j A JX_OBJECT. */
void jx_export( struct jx *j );

#endif
