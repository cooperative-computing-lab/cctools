/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CCTOOLS_ENDIAN_H
#define CCTOOLS_ENDIAN_H

#include <stdint.h>


/** @file cctools_endian.h Byte order conversions.
 *
 * Posix includes functions for performing byte order conversions
 * for 16 and 32 bit values (htonl(), htons(), ntohl(), ntohs()),
 * but there is no standard 64 bit version. Non-standard endian
 * conversion functions are available on various platforms under
 * <endian.h>, <sys/endian.h>, <Endian.h>, etc. This module
 * provides a portable implementation of htonll() and ntohll()
 * for 64 bit endian conversions.
 */

uint64_t cctools_htonll(uint64_t hostlonglong);
uint64_t cctools_ntohll(uint64_t netlonglong);

/* Some platforms provide their own htonll/ntohll functions
 * (or #define them to a byte swap function). We transparently
 * modify the linker namespace to point to our implementation.
 */
#undef  htonll
#undef  ntohll
#define htonll(x) cctools_htonll(x)
#define ntohll(x) cctools_htonll(x)

#endif
