#ifndef DATASWARM_FILE_H
#define DATASWARM_FILE_H

#include "jx.h"

int dataswarm_declare_file(struct jx *j);
struct jx *dataswarm_commit_file(int uuid);
struct jx *dataswarm_delete_file(int uuid);

#endif
