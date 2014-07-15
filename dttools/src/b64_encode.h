/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef URL_DECODE_H
#define URL_ENCODE_H

#include <stddef.h>

int b64_encode(const char *input, size_t len, char *output, size_t buf_len);

#endif
