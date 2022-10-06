/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** @file jx_match.h Unwrap JX types
 *
 * The functions in this header are intended to make JX types more usable
 * from C code. All unwrap JX values in some manner, allowing access to
 * native C types. For the arrays, the match function can be
 * used to destructure its arguments, extracting and type-checking
 * components in a single call.
 * The match functions for single values return an int indicating the
 * result of the match. On match failure, 0 is returned, or 1 on success.
 * The match functions can also give the caller a copy of
 * the matched value. For heap-allocated types, the caller is responsible
 * for free()ing/jx_delete()ing the copied values. In the case that the
 * caller requested a heap-allocated copy but malloc() fails, the match
 * as a whole will fail. This ensures that it is always safe to dereference
 * the copy given back, but might falsely indicate an incorrect type. Code
 * that needs to run in out of memory conditions must not request copies.
 *
 * The common use case is that programs will have some stack allocated
 * variables for local use, and would like to read the value in a JX struct.
 *
 * @code
 *     double val;
 *     if (jx_match_double(j, &val)) {
 *         printf("got value %f\n", val);
 *     } else {
 *         printf("not a valid double\n");
 *     }
 * @endcode
 *
 * Here, the return value is used to check that the given JX struct was
 * in fact a double, and to copy the value onto the local function's
 * stack. If a function needs to modify the JX structure, it must
 * access the struct fields directly.
 *
 * There is also a matching function to extract multiple positional
 * values from an array and validate types in a single call.
 * This function has a scanf()-like interface.
 * The array matching function takes a JX struct
 * and a sequence of item specifications. Each item
 * spec includes a JX type and the location of a pointer that will reference
 * the extracted value. The match function
 * processes each specification in turn, stopping on an invalid spec,
 * or NULL, and returning the number of items succesfully matched.
 *
 * @code
 *     jx_int_t a;
 *     double b;
 *     switch (jx_match_array(j, &a, JX_INTEGER, &b, JX_DOUBLE, NULL)) {
 *     case 1:
 *         printf("got int %d\n", a);
 *     case 2:
 *         printf("got %d and %f\n", a, b)
 *     default:
 *         printf("bad match\n");
 *     }
 * @endcode
 *
 * It's also possible to match on a position without looking at the
 * type of the matched value. The pseudo-type JX_ANY is provided
 * for this purpose. Since the type of the value matched by JX_ANY is
 * unknown, the caller might want to use the other match functions to
 * handle whatever cases they're interested in. The following example
 * uses most of the matching functionality.
 *
 * @code
 *     struct jx *a;
 *     struct jx *b;
 *     if (jx_match_array(j, &a, JX_ARRAY, &b, JX_ANY, NULL) == 2) {
 *         printf("got JX_ARRAY at %p\n", a);
 *         if (jx_istype(b, JX_STRING)) {
 *             printf("no longer supported\n");
 *             return 0;
 *         }
 *         jx_int_t val;
 *         if (jx_match_integer(b, &val)) {
 *             return val % 8;
 *         }
 *     }
 * @endcode
 */

#ifndef JX_MATCH_H
#define JX_MATCH_H

#include "jx.h"

#ifndef JX_ANY
#define JX_ANY -1
#endif

/*
 * Macro to test if we're using a GNU C compiler of a specific vintage
 * or later, for e.g. features that appeared in a particular version
 * of GNU C.  Usage:
 *
 * #if __GNUC_PREREQ__(major, minor)
 * ...cool feature...
 * #else
 * ...delete feature...
 * #endif
 */
#ifndef __GNUC_PREREQ__
#ifdef __GNUC__
#define __GNUC_PREREQ__(x, y) \
	((__GNUC__ == (x) && __GNUC_MINOR__ >= (y)) || \
	 (__GNUC__ > (x)))
#else
#define __GNUC_PREREQ__(x, y) 0
#endif
#endif

#ifndef __wur
#if __GNUC_PREREQ__(3, 4)
#define __wur __attribute__((warn_unused_result))
#else
#define __wur
#endif
#endif

#ifndef __sentinel
#if __GNUC_PREREQ__(4, 0)
#define __sentinel __attribute__((sentinel))
#else
#define __sentinel
#endif
#endif

/** Unwrap a boolean value.
 * @param j The JX structure to match.
 * @param v An address to copy the boolean value to, or NULL if copying
 * the value is unnecessary.
 * @return A truth value indicating the result of the match.
 */
int jx_match_boolean(struct jx *j, int *v) __wur;

/** Unwrap an integer value.
 * @param j The JX structure to match.
 * @param v An address to copy the integer value to, or NULL if copying
 * the value is unnecessary.
 * @return A truth value indicating the result of the match.
 */
int jx_match_integer(struct jx *j, jx_int_t *v) __wur;

/** Unwrap a double value.
 * @param j The JX structure to match.
 * @param v An address to copy the double value to, or NULL if copying
 * the value is unnecessary.
 * @return A truth value indicating the result of the match.
 */
int jx_match_double(struct jx *j, double *v) __wur;

/** Unwrap a string value.
 * Since C strings are just pointers to char arrays, the interface
 * here is a little awkward. If the caller has a stack-allocated
 * (char *), this function can allocate a copy of the string value
 * and store the address in the caller's pointer variable, e.g.
 *
 * @code
 *     char *val;
 *     if (jx_match_string(j, &val)) {
 *         printf("got value %s\n", val);
 *     }
 * @endcode
 *
 * @param j The JX structure to match.
 * @param v The address of a (char *) in which to store the address of
 * the newly malloc()ed string, or NULL if copying the value is unnecessary.
 * @return A truth value indicating the result of the match.
 */
int jx_match_string(struct jx *j, char **v) __wur;

/** Unwrap a symbol value.
 * This function accesses the symbol name as a string.
 * See @ref jx_match_string for details.
 *
 * @param j The JX structure to match.
 * @param v The address of a (char *) in which to store the address of
 * the newly malloc()ed symbol name, or NULL
 * if copying the value is unnecessary.
 * @return A truth value indicating the result of the match.
 */
int jx_match_symbol(struct jx *j, char **v) __wur;

/** Destructure an array.
 * This function accepts an arbitrary number of positional specifications
 * to attempt to match. Each specification is of the form
 *
 *     `<address>`, `<jx type>`
 *
 * where `<jx type>` is JX_INTEGER, JX_ANY, etc. and `<address>` is the address
 * to store the matched value. The last argument must be NULL to mark the
 * end of the specifications. The specifications will be matched
 * in the order given, and matching ends on the first failure. If the JX
 * value passed in was not an array, this is considered a failure before
 * any matches succeed, so 0 is returned.
 * @param j The JX structure to match.
 * @return The number of elements successfully matched.
 */
int jx_match_array(struct jx *j, ...) __wur __sentinel;

#endif
