#ifndef DATASWARM_VALIDATE_H
#define DATASWARM_VALIDATE_H

#include "jx.h"

extern const char *DECLARE_FILE[];
extern const char *SUBMIT_TASK[];
extern const char *SUBMIT_SERVICE[];

/* 1 if str in array, 0 otherwise */
int is_in(const char *str, const char **array);

/* 1 if keys of json in array, 0 if at least one is not */
int validate_json(struct jx *json, const char **array);

#endif
