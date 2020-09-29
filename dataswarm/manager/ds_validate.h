#ifndef DATASWARM_VALIDATE_H
#define DATASWARM_VALIDATE_H

#include "jx.h"

const char *DECLARE_FILE[] = { "type", "project", "metadata" };
const char *SUBMIT_TASK[] = { "type", "service", "project", "namespace", "resources", "event" };
const char *SUBMIT_SERVICE[] = { "type", "project", "namespace", "resources", "environment" };

int is_in(const char *str, const char **array);
int validate_json(struct jx *json, const char **array);

#endif
