#ifndef HELPERS_H
#define HELPERS_H

#include "jx.h"

static const char *DECLARE_FILE[] = { "type", "project", "metadata" }
static const char *SUBMIT_TASK[] = { "type", "service", "project", "namespace", "resources", "event" }
static const char *SUBMIT_SERVICE[] = { "type", "project", "namespace", "resources", "environment" }

static int is_in(const char *str, const char **array);
static int validate_json(struct jx *json, const char **array);

#endif
