/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef RANDOM_H
#define RANDOM_H

#include <stdint.h>
#include <stdlib.h>

/** @file random.h A PRNG library.
 */

/** Initialize the random number generator.
 *
 * Uses system PRNG devices to seed the library PRNG.
 */
void    random_init (void);

/** Get a random int.
 *
 * @return a random int.
 */
#define random_int()   ((int) random_int64())

/** Get a random unsigned int.
 *
 * @return a random unsigned int.
 */
#define random_uint()   ((unsigned) random_int64())

/** Get a random int32_t.
 *
 * @return a random int32_t.
 */
#define random_int32() ((int32_t) random_int64())

/** Get a random int64_t.
 *
 * @return a random int64_t.
 */
int64_t random_int64 (void);

/** Get a random double from (0, 1)
 *
 * @return a random double from (0, 1)
 */
double random_double (void);

/** Insert random data into an array.
 *
 * @param m the memory to fill.
 * @param l the length of the m.
 */
void    random_array (void *m, size_t l);

/** Insert a random string in hexadecimal.
 *
 * @param s the location in the string.
 * @param l the number of characters to insert. Includes NUL byte!
 */
void    random_hex   (char *s, size_t l);

#endif

/* vim: set noexpandtab tabstop=8: */
