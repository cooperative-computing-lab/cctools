#ifndef WORK_QUEUE_JSON_H
#define WORK_QUEUE_JSON_H

#include "work_queue.h"
#include "jx.h"

struct work_queue_task* work_queue_task_json_create(const char* str);

struct work_queue* work_queue_json_create(const char* str);

int work_queue_json_submit(struct work_queue *q, const char* str);

#endif
