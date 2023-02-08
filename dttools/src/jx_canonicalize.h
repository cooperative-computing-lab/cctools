/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_CANONICALIZE_H
#define JX_CANONICALIZE_H

/** @file jx_canonicalize.h Print a JX structure in canonical form.
 * Canonical form is not particularly readable, e.g. there is no added
 * whitespace and floats are printed with fixed precision in exponential form.
 * Only the plain JSON types are allowed.
 * Objects must have unique string keys.
 */

#include <stdio.h>
#include <stdbool.h>

#include "jx.h"
#include "link.h"
#include "buffer.h"

/** Canonicalize a JX expression to a string.
 * The caller must free() the returned string.
 * @param j A JX expression.
 * @returns NULL With an invalid JX structure.
 */
char *jx_canonicalize(struct jx *j);

#endif
