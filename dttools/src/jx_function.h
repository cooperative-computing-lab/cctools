/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_FUNCTION_H
#define JX_FUNCTION_H

#include "jx.h"
#include "jx_eval.h"

const char *jx_function_name_to_string(jx_function_t func);
jx_function_t jx_function_name_from_string(const char *name);
struct jx *jx_function_range(struct jx_function *f, struct jx *context, jx_eval_mode_t mode, struct jx *default_value);
struct jx *jx_function_foreach(struct jx_function *f, struct jx *context, jx_eval_mode_t mode, struct jx *default_value);
struct jx *jx_function_str(struct jx_function *f, struct jx *context, jx_eval_mode_t mode, struct jx *default_value);
struct jx *jx_function_join(struct jx_function *f, struct jx *context, jx_eval_mode_t mode, struct jx *default_value);
struct jx *jx_function_dbg(struct jx_function *f, struct jx *context, jx_eval_mode_t mode, struct jx *default_value);

#endif
