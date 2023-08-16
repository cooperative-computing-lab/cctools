/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "uuid.h"

#include "debug.h"
#include "random.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

static void setuuid (cctools_uuid_t *uuid, unsigned char *bytes)
{
	bytes[6] = (bytes[6]|0x40) & 0x4f; /* most sig. 4 bits are 0b0100 */
	bytes[8] = (bytes[8]|0x80) & 0xbf; /* most sig. 2 bits are 0b10 */
	snprintf(uuid->str, sizeof uuid->str, "%02X%02X%02X%02X-%02X%02X-%02X%02X-%02X%02X-%02X%02X%02X%02X%02X%02X", (int)bytes[0], (int)bytes[1], (int)bytes[2], (int)bytes[3], (int)bytes[4], (int)bytes[5], (int)bytes[6], (int)bytes[7], (int)bytes[8], (int)bytes[9], (int)bytes[10], (int)bytes[11], (int)bytes[12], (int)bytes[13], (int)bytes[14], (int)bytes[15]);
}

void cctools_uuid_create (cctools_uuid_t *uuid)
{
	unsigned char bytes[16*2];
	random_array(bytes, sizeof bytes);
	setuuid(uuid, bytes);
}

void cctools_uuid_loadhex (cctools_uuid_t *uuid, const char *hex)
{
	unsigned char bytes[16*2];
	size_t k;
	assert(strlen(hex) >= sizeof bytes);
	for (k = 0; k < 16; k += 1, hex += 2) {
		char byte[3] = {hex[0], hex[1], '\0'};
		char *e;
		unsigned long value = strtoul(byte, &e, 16);
		if (e == &byte[2]) {
			bytes[k] = value;
		} else {
			fatal("bad hex source");
		}
	}
	setuuid(uuid, bytes);
}

/* vim: set noexpandtab tabstop=8: */
