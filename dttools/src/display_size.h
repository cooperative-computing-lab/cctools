/*
 * Copyright (C) 2012- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

#ifndef DISPLAY_SIZE_H
#define DISPLAY_SIZE_H

#include "int_sizes.h"
#include "stdlib.h"
#include "string.h"
#include "stdio.h"

/** @file display_size.h Display file sizes in human readable format i.e. with units **/

char * human_readable_size(UINT64_T size);

#endif
