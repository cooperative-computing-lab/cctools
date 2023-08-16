/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "b64.h"
#include "buffer.h"
#include "catch.h"
#include "debug.h"

#include <assert.h>
#include <stddef.h>

int b64_encode(const void *blob, size_t bloblen, buffer_t *Bb64)
{
	static const char e_base64[64] = {
		'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
		'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
		'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
		'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
		'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
		'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
		'w', 'x', 'y', 'z', '0', '1', '2', '3',
		'4', '5', '6', '7', '8', '9', '+', '/'
	};

	int rc;
	const unsigned char *i = blob;
	char o[4];
	size_t l;

	for (l = bloblen; l >= 3; l -= 3, i += 3) {
		o[0] = e_base64[(i[0]>>2)&0x3f];
		o[1] = e_base64[(((i[0]<<4)&0x30)|((i[1]>>4)&0xf))&0x3f];
		o[2] = e_base64[(((i[1]<<2)&0x3c)|((i[2]>>6)&0x3))&0x3f];
		o[3] = e_base64[i[2]&0x3f];
		CATCHUNIX(buffer_putlstring(Bb64, o, sizeof(o)));
	}

	if (l > 0) {
		assert(l == 1 || l == 2);
		o[0] = e_base64[(i[0]>>2)&0x3f];
		if (l == 1) {
			o[1] = e_base64[(((i[0]<<4)&0x30)|((0>>4)&0xf))&0x3f];
			o[2] = '=';
		} else {
			o[1] = e_base64[(((i[0]<<4)&0x30)|((i[1]>>4)&0xf))&0x3f];
			o[2] = e_base64[(((i[1]<<2)&0x3c)|(0&0x3))&0x3f];
		}
		o[3] = '=';
		CATCHUNIX(buffer_putlstring(Bb64, o, sizeof(o)));
	}

	rc = 0;
	goto out;
out:
	return RCUNIX(rc);
}

int b64_decode (const char *b64, buffer_t *Bblob)
{
	static const int d_base64[256] = {
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1, 0x3e,   -1,   -1,   -1, 0x3f,
		0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b,
		0x3c, 0x3d,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
		0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
		0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
		0x17, 0x18, 0x19,   -1,   -1,   -1,   -1,   -1,
		  -1, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
		0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30,
		0x31, 0x32, 0x33,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,
		  -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1
	};

	int rc;

	for (; b64[0]; b64 += 4) {
		char o[3];
		unsigned char i[4] = {(unsigned char)b64[0], (unsigned char)b64[1], 0, 0};
		size_t l = 3;
		if (d_base64[i[0]] == -1 || d_base64[i[1]] == -1)
			return errno = EINVAL, -1;
		if (b64[2] == 0 || b64[3] == 0)
			return errno = EINVAL, -1;
		i[3] = b64[3] == '=' ? (l = 2, 'A') : b64[3];
		i[2] = b64[2] == '=' ? (l = 1, 'A') : b64[2];
		if (d_base64[i[2]] == -1 || d_base64[i[3]] == -1)
			return errno = EINVAL, -1;

		o[0] = ((d_base64[i[0]]<<2)&0xfc) | ((d_base64[i[1]]>>4)&0x03);
		o[1] = ((d_base64[i[1]]<<4)&0xf0) | ((d_base64[i[2]]>>2)&0x0f);
		o[2] = ((d_base64[i[2]]<<6)&0xc0) | ((d_base64[i[3]])&0x3f);

		CATCHUNIX(buffer_putlstring(Bblob, o, l));

		if (l < 3)
			break; /* end of b64 */
	}

	rc = 0;
	goto out;
out:
	return RCUNIX(rc);
}

/* vim: set noexpandtab tabstop=8: */
