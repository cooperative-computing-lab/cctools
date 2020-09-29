#ifndef DATASWARM_VALIDATE_H
#define DATASWARM_VALIDATE_H

#include "jx.h"

extern const char *DECLARE_FILE[];
extern const char *SUBMIT_TASK[];
extern const char *SUBMIT_SERVICE[];

int is_in(const char *str, const char **array);
int validate_json(struct jx *json, const char **array);

#endif
