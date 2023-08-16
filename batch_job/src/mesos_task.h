#ifndef MESOS_TASK_H_
#define MESOS_TASK_H_

#include "stdlib.h"
#include "text_list.h"
#include "xxmalloc.h"
#include "path.h"
#include "stringtools.h"

// mesos task struct
struct mesos_task{
    int task_id;
    char *task_cmd;
    struct text_list *task_input_files;
    struct text_list *task_output_files;
};

struct mesos_task *mesos_task_create(int task_id, const char *cmd, \
        const char *extra_input_files, const char *extra_output_files);

void mesos_task_delete(struct mesos_task *mt);

#endif

/* vim: set noexpandtab tabstop=8: */
