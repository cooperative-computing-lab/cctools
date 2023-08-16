/*
Copyright (C) 2022 The University of Notre Dame
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
#include <inttypes.h>

/** JX atomic type.  */
typedef enum {
	JX_NULL = 0, /**< null value */
	JX_BOOLEAN,  /**< true or false */
	JX_INTEGER,  /**< integer value */
	JX_DOUBLE,   /**< floating point value */
	JX_STRING,   /**< string value */
	JX_SYMBOL,   /**< variable identifier */
	JX_ARRAY,    /**< array containing values */
	JX_OBJECT,   /**< object containing key-value pairs */
	JX_OPERATOR, /**< operator on multiple values. */
	JX_ERROR,    /**< indicates failed evaluation */
} jx_type_t;

typedef int64_t jx_int_t;
#define PRIiJX PRIi64

struct jx_comprehension {
	unsigned line;
	char *variable; /**< variable for comprehension */
	struct jx *elements; /**< items for list comprehension */
	struct jx *condition; /**< condition for filtering list comprehension */
	struct jx_comprehension *next;
};

/** JX item linked-list used by @ref JX_ARRAY and @ref jx.items */

struct jx_item {
	unsigned line;
	struct jx *value;       /**< value of this item */
	struct jx_comprehension *comp;
	struct jx_item *next;	/**< pointer to next item */
};

/** JX key-value pairs used by @ref JX_OBJECT and @ref jx.pairs */

struct jx_pair {
	unsigned line;
	struct jx      *key;	/**< key of this pair */
	struct jx      *value;  /**< value of this pair */
	struct jx_comprehension *comp;
	struct jx_pair *next;   /**< pointer to next pair */
};

typedef enum {
	JX_OP_EQ,
	JX_OP_NE,
	JX_OP_LE,
	JX_OP_LT,
	JX_OP_GE,
	JX_OP_GT,
	JX_OP_ADD,
	JX_OP_SUB,
	JX_OP_MUL,
	JX_OP_DIV,
	JX_OP_MOD,
	JX_OP_AND,
	JX_OP_OR,
	JX_OP_NOT,
	JX_OP_LOOKUP,
	JX_OP_CALL,
	JX_OP_SLICE,
	JX_OP_DOT,
	JX_OP_INVALID,
} jx_operator_t;

struct jx_operator {
	jx_operator_t type;
	unsigned line;
	struct jx *left;
	struct jx *right;
};

/** JX value representing any expression type. */

struct jx {
	jx_type_t type;               /**< type of this value */
	unsigned line;                /**< line where this value was defined */
	union {
		int boolean_value;      /**< value of @ref JX_BOOLEAN */
		jx_int_t integer_value; /**< value of @ref JX_INTEGER */
		double double_value;   /**< value of @ref JX_DOUBLE */
		char * string_value;  /**< value of @ref JX_STRING */
		char * symbol_name;   /**< value of @ref JX_SYMBOL */
		struct jx_item *items;  /**< value of @ref JX_ARRAY */
		struct jx_pair *pairs;  /**< value of @ref JX_OBJECT */
		struct jx_operator oper; /**< value of @ref JX_OPERATOR */
		struct jx *err;  /**< error value of @ref JX_ERROR */
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

/** Create a JX string value. @param string_value A C string, which will be duplicated via strdup(). @return A JX string value. */
struct jx * jx_string( const char *string_value );

/** Create a JX string value without copying (uncommon). @param string_value A C string, which will be *not* be duplicated, but will be freed at object deletion.  @return A JX string value. */
struct jx * jx_string_nocopy( char *string_value );

/** Create a JX string value using prinf style formatting.  @param fmt A printf-style format string, followed by matching arguments.  @return A JX string value. */
struct jx * jx_format( const char *fmt, ... );

/** Create a JX symbol.
Note that symbols are an extension to the JSON standard. A symbol is a reference to an external variable, which can be resolved by using @ref jx_eval
@param symbol_name A C string.
@return A JX expression.
*/
struct jx * jx_symbol( const char *symbol_name );

/** Create a JX_ERROR.
@param err The associated data for the error. This should be a string description of the error.
@return A JX error value.
*/
struct jx * jx_error( struct jx *err );

/** Create a JX array.  @param items A linked list of @ref jx_item values.  @return A JX array. */
struct jx * jx_array( struct jx_item *items );

/** Create a JX array with inline items.  @param value One or more items of the array must be given, terminated with a null value.  @return A JX array. */
struct jx * jx_arrayv( struct jx *value, ... );

/** Create a JX object.  @param pairs A linked list of @ref jx_pair key-value pairs.  @return a JX object. */
struct jx * jx_object( struct jx_pair *pairs );

/** Create a JX object. Arguments are alternating string key -- *jx values. Must be termianted with a null value.
 * This is syntactic sugar for jx_object(jx_pair(jx_string(k), v, jx_pair(jx_string(k), v, jx_pair( etc.))));
 *
 * struct jx *j = jx_objectv("A", jx_integer(42),                  // key and value of A,
 *                           "B", jx_objectv("C", jx_string("xyz") // key and value of B
 *                           NULL));                               // terminating null value.
 * @param key Name of the first property
 * @param value Value for the first property
 * @param ...  Alternating key-value pairs
 * @return a JX object*/
struct jx * jx_objectv( const char *key, struct jx *value, ... );

/** Create a JX binary expression, @param oper The kind of operator.  @param left The left side of the expression.  @param right The right side of the expression. */
struct jx * jx_operator( jx_operator_t oper, struct jx *left, struct jx *right );

/** Create a JX key-value pair.  @param key The key. @param value The value. @param next The next item in the linked list. @return A key-value pair.*/
struct jx_pair * jx_pair( struct jx *key, struct jx *value, struct jx_pair *next );

/** Create a JX array item.  @param value The value of this item.  @param next The next item in the linked list.  @return An array item. */
struct jx_item * jx_item( struct jx *value, struct jx_item *next );

/** Create a JX comprehension.
 * @param variable The variable name to bind.
 * @param elements The elements to bind.
 * @param condition The boolean filter to evaluate.
 * @param next Nested comprehension(s).
 * @returns A JX comprehension.
 */
struct jx_comprehension *jx_comprehension(const char *variable, struct jx *elements, struct jx *condition, struct jx_comprehension *next);

/** Test an expression's type.  @param j An expression. @param type The desired type. @return True if the expression type matches, false otherwise. */
int jx_istype( struct jx *j, jx_type_t type );

/** Test for an atomic value. @param j An expression.  @return True if the expression is an atomic integer, float, string or boolean. */
int jx_isatomic( struct jx *j );

/** Test an expression for the boolean value TRUE.  @param j An expression to test.  @return True if the expression is boolean and true. */
int jx_istrue( struct jx *j );

/** Test an expression for the boolean value FALSE @param j An expression to test.  @return True if the expression is boolean and false. */
int jx_isfalse( struct jx *j );

int jx_comprehension_equals(struct jx_comprehension *j, struct jx_comprehension *k);
int jx_item_equals(struct jx_item *j, struct jx_item *k);
int jx_pair_equals(struct jx_pair *j, struct jx_pair *k);

/** Test two expressions for equality. @param j A constant expression. @param k A constant expression. @return True if equal, false if not.
*/
int jx_equals( struct jx *j, struct jx *k );

/** Get the length of an array. Returns -1 if array is null or not an array. @param array The array to check. */
int jx_array_length( struct jx *array );

struct jx_comprehension *jx_comprehension_copy(struct jx_comprehension *c);
struct jx_item *jx_item_copy(struct jx_item *i);
struct jx_pair *jx_pair_copy(struct jx_pair *p);

/** Duplicate an expression. @param j An expression. @return A copy of the expression, which must be deleted by @ref jx_delete
*/
struct jx * jx_copy( struct jx *j );

/** Delete an expression recursively. @param j An expression to delete. */
void jx_delete( struct jx *j );

/** Delete a key-value pair.  @param p The key-value pair to delete. */
void jx_pair_delete( struct jx_pair *p );

/** Delete an array item.  @param i The array item to delete. */
void jx_item_delete( struct jx_item *i );

/** Delete a comprehension. @param comp The comprehension to delete. */
void jx_comprehension_delete(struct jx_comprehension *comp);

/** Remove a key-value pair from an object.  @param object The object.  @param key The key. @return The corresponding value, or null if it is not present. */
struct jx * jx_remove( struct jx *object, struct jx *key );

/** Insert a key-value pair into an object.  @param object The object.  @param key The key.  @param value The value. @return True on success, false on failure.  Failure can only occur if the object is not a @ref JX_OBJECT. */
int jx_insert( struct jx *object, struct jx *key, struct jx *value );

/** Insert a key-value pair into an object, unless the value is an empty collection, in which case delete the key and value.  @param object The target object. @param key The key.  @param value The value. @return 1 on success, -1 on empty value, 0 on failure.  Failure can only occur if the object is not a @ref JX_OBJECT. */
int jx_insert_unless_empty( struct jx *object, struct jx *key, struct jx *value );

/** Insert a boolean value into an object @param object The object @param key The key represented as a C string  @param value The boolean value. */
void jx_insert_boolean( struct jx *object, const char *key, int value );

/** Insert an integer value into an object @param object The object @param key The key represented as a C string  @param value The integer value. */
void jx_insert_integer( struct jx *object, const char *key, jx_int_t value );

/** Insert a double value into an object @param object The object @param key The key represented as a C string  @param value The double value. */
void jx_insert_double( struct jx *object, const char *key, double value );

/** Insert a string value into an object @param object The object @param key The key represented as a C string  @param value The C string value. */
void jx_insert_string( struct jx *object, const char *key, const char *value );

/** Search for a arbitrary item in an object.  The key is an ordinary string value.  @param object The object in which to search.  @param key The string key to match.  @return The value of the matching pair, or null if none is found. */
struct jx * jx_lookup( struct jx *object, const char *key );

/* Like @ref jx_lookup, but found is set to 1 when the key is found. Useful for when value is false. */
struct jx * jx_lookup_guard( struct jx *j, const char *key, int *found );

/** Search for a string item in an object.  The key is an ordinary string value. @param object The object in which to search.  @param key The string key to match.  @return The C string value of the matching object, or null if it is not found, or is not a string. */
const char * jx_lookup_string( struct jx *object, const char *key );

/** Search for a string item in an object.  Behaves the same as jx_lookup_string, but returns a duplicated copy.  The result must be deallocated with free(). @param object The object in which to search.  @param key The string key to match.  @return The C string value of the matching object, or null if it is not found, or is not a string. */
char * jx_lookup_string_dup( struct jx *object, const char *key );

/** Search for an integer item in an object.  The key is an ordinary string value.  @param object The object in which to search.  @param key The string key to match.  @return The integer value of the matching object, or zero if it is not found, or is not an integer. */
jx_int_t jx_lookup_integer( struct jx *object, const char *key );

/** Search for a boolean item in an object.  The key is an ordinary string value.  @param object The object in which to search.  @param key The string key to match.  @return One if the value of the matching object is true, or zero if it is false, not found, or not a boolean. */
int jx_lookup_boolean( struct jx *object, const char *key );

/** Search for a double item in an object.  The key is an ordinary string value.  @param object The object in which to search.  @param key The string key to match.  @return The double value of the matching object, or null if it is not found, or is not a double. */
double jx_lookup_double( struct jx *object, const char *key );

/** Insert an item at the beginning of an array.  @param array The array to modify.  @param value The value to insert. */
void jx_array_insert( struct jx *array, struct jx *value );

/** Append an item at the end of an array.  @param array The array to modify.  @param value The value to append. */
void jx_array_append( struct jx *array, struct jx *value );

/** Get the nth item in an array.  @param array The array to search.  @param nth The index of the desired value. @return The nth element, or NULL if the index is out of bounds. */
struct jx * jx_array_index( struct jx *array, int nth );

/** Concatenate the given arrays into a single array. The passed arrays are consumed. @param array An array to concatenate. The list of arrays must be terminated by NULL. */
struct jx *jx_array_concat( struct jx *array, ...);

/** Remove and return the first element in the array.
 * @param array The JX_ARRAY to update.
 * @returns The first value in array, or NULL if array is empty or not a JX_ARRAY. */
struct jx *jx_array_shift(struct jx *array);

/** Determine if an expression is constant.  Traverses the expression recursively, and returns true if it consists only of constant values, arrays, and objects. @param j The expression to evaluate.  @return True if constant. */
int jx_is_constant( struct jx *j );

/** Export a jx object into the current environment using setenv(). @param j A JX_OBJECT. */
void jx_export( struct jx *j );

/** Iterate over the values in an array.
 * The iteration state is stored by the caller in an opaque pointer variable. When starting iteration,
 * the caller MUST pass the address of a pointer initialized to NULL as i. It is undefined behavior
 * to pass a non-NULL iterator variable not set by a previous call. Subsequent calls should
 * use the same variable to continue iteration. After the initial call, the value of j is ignored.
 *
 *     struct jx *item;
 *     for (void *i = NULL; (item = jx_iterate_array(j, &i));) {
 *         printf("array item: ");
 *         jx_print_stream(item, stdout);
 *         printf("\n");
 *     }
 *
 * @param j The JX_ARRAY to iterate over.
 * @param i A variable to store the iteration state.
 * @return A pointer to each item in the array, and NULL when iteration is finished.
 */
struct jx * jx_iterate_array(struct jx *j, void **i);

/** Iterate over the values in an object.
 * The iteration state is stored by the caller in an opaque pointer variable. When starting iteration,
 * the caller MUST pass the address of a pointer initialized to NULL as i. It is undefined behavior
 * to pass a non-NULL iterator variable not set by a previous call. Subsequent calls should
 * use the same variable to continue iteration. After the initial call, the value of j is ignored.
 *
 *     struct jx *item;
 *     void *i = NULL;
 *     while ((item = jx_iterate_values(j, &i))) {
 *         printf("object value: ");
 *         jx_print_stream(item, stdout);
 *         printf("\n");
 *     }
 *
 * @param j The JX_OBJECT to iterate over.
 * @param i A variable to store the iteration state.
 * @return A pointer to each value in the object, and NULL when iteration is finished.
 */
struct jx * jx_iterate_values(struct jx *j, void **i);

/** Iterate over the keys in an object.
 * The iteration state is stored by the caller in an opaque pointer variable. When starting iteration,
 * the caller MUST pass the address of a pointer initialized to NULL as i. It is undefined behavior
 * to pass a non-NULL iterator variable not set by a previous call. Subsequent calls should
 * use the same variable to continue iteration. After the initial call, the value of j is ignored.
 *
 *     struct jx *item;
 *     void *i = NULL;
 *     while ((item = jx_iterate_keys(j, &i))) {
 *         printf("object key: ");
 *         jx_print_stream(item, stdout);
 *         printf("\n");
 *     }
 *
 * @param j The JX_OBJECT to iterate over.
 * @param i A variable to store the iteration state.
 * @return A pointer to each key in the object, and NULL when iteration is finished.
 */
const char *jx_iterate_keys(struct jx *j, void **i);

/* Get the current key while iterating over an object.
 * The iteration variable must have been passed to jx_iterate_keys
 * or jx_iterate_values. This directly fetches the current key rather than
 * doing a lookup from the beginning, so it takes constant time and
 * can handle repeated keys.
 */
const char *jx_get_key(void **i);

/* Get the current value while iterating over an object.
 * The iteration variable must have been passed to jx_iterate_keys
 * or jx_iterate_values. This directly fetches the current value rather than
 * doing a lookup from the beginning, so it takes constant time and
 * can handle repeated keys.
 */
struct jx *jx_get_value(void **i);


/** Merge an arbitrary number of JX_OBJECTs into a single new one. The constituent objects are not consumed. Objects are merged in the order given, i.e. a key can replace an identical key in a preceding object. The last argument must be NULL to mark the end of the list. @return A merged JX_OBJECT that must be deleted with jx_delete. */
struct jx *jx_merge(struct jx *j, ...);

#endif

/*vim: set noexpandtab tabstop=8: */
