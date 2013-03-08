/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef LINK_NVPAIR_H
#define LINK_NVPAIR_H

#include "link.h"
#include "nvpair.h"

/** Read an nvpair to a link in text format.
@param l The link to read from.
@param stoptime The absolute time at which to stop.
@return The nvpair read, or null in the case of a timeout or end-of-stream.
*/

struct nvpair *link_nvpair_read( struct link *l, time_t stoptime );

/** Write an nvpair to a link in text format.
@param l The link to write to.
@param stoptime The absolute time at which to stop.
*/

void link_nvpair_write( struct link *l, struct nvpair *nv, time_t stoptime );

#endif
