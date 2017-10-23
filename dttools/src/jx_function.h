/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_FUNCTION_H
#define JX_FUNCTION_H

#include "jx.h"

struct jx *jx_function_range(struct jx *args);
struct jx *jx_function_format(struct jx *args);
struct jx *jx_function_join(struct jx *args);
struct jx *jx_function_ceil(struct jx *args);
struct jx *jx_function_floor(struct jx *args);

#endif
