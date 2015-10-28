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

/** JX atomic type.  */

typedef enum {
	JX_NULL,	/**< null value */
	JX_BOOLEAN,	/**< true or false */
	JX_INTEGER,	/**< integer value */
	JX_FLOAT,	/**< floating point value */
	JX_STRING,	/**< string value */
	JX_SYMBOL,	/**< variable identifier */
	JX_ARRAY,	/**< array containing values */
	JX_OBJECT,	/**< object containing key-value pairs */
} jx_type_t;

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
		int boolean_value;    /**< value of @ref JX_BOOLEAN */
		int integer_value;    /**< value of @ref JX_INTEGER */
		double float_value;   /**< value of @ref JX_FLOAT */
		char * string_value;  /**< value of @ref JX_STRING */
		char * symbol_name;   /**< value of @ref JX_SYMBOL */
		struct jx_item *items;  /**< value of @ref JX_ARRAY */
		struct jx_pair *pairs;  /**< value of @ref JX_OBJECT */
	};
};

/** Create a JX null value. @return A JX expression. */
struct jx * jx_null();

/** Create a JX boolean value.  @param boolean_value A C boolean value.  @return A JX boolean value.*/
struct jx * jx_boolean( int boolean_value );

/** Create a JX integer value. @param integer_value A C integer. @return a JX integer value. */
struct jx * jx_integer( int integer_value );

/** Create a JX floating point value. @param float_value A C double precision floating point.  @return a JX float value. */
struct jx * jx_float( double float_value );

/** Create a JX string value. @param string_value A C string. @return A JX string value. */
struct jx * jx_string( const char *string_value );

/** Create a JX symbol. Note that symbols are an extension to the JSON standard. A symbol is a reference to an external variable, which can be resolved by using @ref jx_evaluate. @param symbol_name A C string. @return A JX expression.
*/
struct jx * jx_symbol( const char *symbol_name );

/** Create a JX array.  @param items A linked list of @ref jx_item values.  @return A JX array. */
struct jx * jx_array( struct jx_item *items );

/** Create a JX object.  @param pairs A linked list of @ref jx_pair key-value pairs.  @return a JX object. */
struct jx * jx_object( struct jx_pair *pairs );

/** Create a JX key-value pair.  @param key The key. @param value The value. @param next The next item in the linked list. @return A key-value pair.*/
struct jx_pair * jx_pair( struct jx *key, struct jx *value, struct jx_pair *next );

/** Create a JX array item.  @param value The value of this item.  @param next The next item in the linked list.  @return An array item. */
struct jx_item * jx_item( struct jx *value, struct jx_item *next );

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

/** Insert a key-value pair into an object.  @param object The object.  @param key The key.  @param value The value. @return True on success, false on failure.  Failure can only occur if the object is not a @ref JX_OBJECT. */
int jx_object_insert( struct jx *object, struct jx *key, struct jx *value );

/** Search for a particular item in an object.  The key is an ordinary string value.  (To search by other expression values, see @ref jx_object_lookup_expr.  @param object The object in which to search.  @param key The string key to match.  @return The value of the matching pair, or null if none is found. */
struct jx * jx_object_lookup( struct jx *object, const char *key );

/** Search for a particular item in an object.  The key may be any JX compound expression.  @param object The object in which to search.  @param key The key to match.  @return The value of the matching pair, or null if none is found. */
struct jx * jx_object_lookup_expr( struct jx *object, struct jx *key );

/** Insert an item at the beginning of an array.  @param array The array to modify.  @param value The value to insert. */
void jx_array_insert( struct jx *array, struct jx *value );

/** Append an item at the end of an array.  @param array The array to modify.  @param value The value to append. */
void jx_array_append( struct jx *array, struct jx *value );

/** Determine if an expression is constant.  Traverses the expression recursively, and returns true if it consists only of constant values, arrays, and objects. @param j The expression to evaluate.  @return True if constant. */
int jx_is_constant( struct jx *j );

/** Evaluation function.  To use @ref jx_evaluate, the caller must
define a function of type @ref jx_eval_func_t which accepts a symbol
name and returns a JX value.
*/
typedef struct jx * (*jx_eval_func_t) ( const char *ident );

/** Evaluate an expression.  Traverses the expression recursively, and
for each value of type @ref JX_SYMBOL, invokes the evaluator function
to replace it with a constant value.  @param j The expression to evaluate.  @param evaluator The evaluating function.  @return A newly created result expression, which must be deleted with @ref jx_delete.
*/
struct jx * jx_evaluate( struct jx *j, jx_eval_func_t evaluator );

#endif
