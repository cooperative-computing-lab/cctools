/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef NVPAIR_PRIVATE_H
#define NVPAIR_PRIVATE_H

/*
The definition of an nvpair object is shared among nvpair.h
and nvpair_history.h, but is not generally a public interface.
*/

struct nvpair {
	struct hash_table *table;
};

#endif

