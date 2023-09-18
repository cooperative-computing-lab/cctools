/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdint.h>

#include "cctools_endian.h"

uint64_t cctools_htonll(uint64_t hostlonglong)
{
	uint64_t out = 0;
	uint8_t *d = (uint8_t *)&out;
	d[7] = hostlonglong >> 0;
	d[6] = hostlonglong >> 8;
	d[5] = hostlonglong >> 16;
	d[4] = hostlonglong >> 24;
	d[3] = hostlonglong >> 32;
	d[2] = hostlonglong >> 40;
	d[1] = hostlonglong >> 48;
	d[0] = hostlonglong >> 56;
	return out;
}

uint64_t cctools_ntohll(uint64_t netlonglong)
{
	uint64_t out = 0;
	uint8_t *d = (uint8_t *)&netlonglong;
	out |= (uint64_t)d[7] << 0;
	out |= (uint64_t)d[6] << 8;
	out |= (uint64_t)d[5] << 16;
	out |= (uint64_t)d[4] << 24;
	out |= (uint64_t)d[3] << 32;
	out |= (uint64_t)d[2] << 40;
	out |= (uint64_t)d[1] << 48;
	out |= (uint64_t)d[0] << 56;
	return out;
}
