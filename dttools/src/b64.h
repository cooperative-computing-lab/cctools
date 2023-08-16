/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef B64_H
#define B64_H

#include "buffer.h"

#include <stddef.h>

/** @file b64.h  Base64 Encoding Operations.
 *
 */

/** Compute the size of the base64 encoding for a blob.
 *
 * This function returns the length of a base64 encoding for the given blob
 * length, bloblen. This length includes a NUL terminator.
 *
 * @param bloblen The input length of the binary blob.
 */
static inline size_t b64_size (size_t bloblen)
{
	/* Ceil division by 3 multiplied by 4 */
	return 1 /* NUL byte */ + (bloblen + 3 - 1) / 3 * 4;
}

/** Encode a binary blob in base64.
 *
 * The character array pointed to by b64 must have length of
 * at least b64_size(bloblen).
 *
 * @param blob The input binary blob.
 * @param bloblen The input length of the binary blob.
 * @param b64 The output base64 encoded blob.
 * @return 0 on success, -1+errno on error.
 */
int b64_encode(const void *blob, size_t bloblen, buffer_t *b64);

/** Decode a base64 encoded blob.
 *
 * The character array pointed to by b64 must have length of
 * at least b64_size(bloblen).
 *
 * @param b64 The input base64 encoded blob.
 * @param blob The output binary blob.
 * @return 0 on success, -1+errno on error.
 */
int b64_decode(const char *b64, buffer_t *blob);

#endif /* B64_H */

/* vim: set noexpandtab tabstop=8: */
