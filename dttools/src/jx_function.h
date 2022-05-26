/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_FUNCTION_H
#define JX_FUNCTION_H

#include "jx.h"
#include <stdio.h>

struct jx *jx_function_eval(const char *funcname, struct jx *args, struct jx *ctx);
struct jx *jx_function_sub(const char *funcname, struct jx *args, struct jx *ctx);
void       jx_function_help(FILE *file);

struct jx *jx_function_range(struct jx *args);
struct jx *jx_function_format(struct jx *args);
struct jx *jx_function_join(struct jx *args);
struct jx *jx_function_ceil(struct jx *args);
struct jx *jx_function_floor(struct jx *args);
struct jx *jx_function_basename(struct jx *args);
struct jx *jx_function_dirname(struct jx *args);
struct jx *jx_function_listdir(struct jx *args);
struct jx *jx_function_escape(struct jx *args);
struct jx *jx_function_template(struct jx *args, struct jx *ctx);
struct jx *jx_function_len(struct jx *args);
struct jx *jx_function_fetch(struct jx *args);
struct jx *jx_function_select(struct jx *args, struct jx *ctx);
struct jx *jx_function_project(struct jx *args, struct jx *ctx);
struct jx *jx_function_schema(struct jx *args);
struct jx *jx_function_like(struct jx *args);
struct jx *jx_function_keys(struct jx *args);
struct jx *jx_function_values(struct jx *args);
struct jx *jx_function_items(struct jx *args);

#endif
