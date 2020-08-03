#ifndef HELPERS_H
#define HELPERS_H

#include "jx.h"

static int is_in(const char *str, const char **array);
static int validate_json(struct jx *json, const char **array);

#endif
