/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This is a transitional interface to allow for conversions
between jx and nvpair, until the former has fully replaced
the latter.
*/

#ifndef NVPAIR_JX_H
#define NVPAIR_JX_H

struct nvpair * jx_to_nvpair( struct jx *j );
struct jx * nvpair_to_jx( struct nvpair *nv );
struct jx * jx_parse_nvpair_file( const char *path );

#endif
