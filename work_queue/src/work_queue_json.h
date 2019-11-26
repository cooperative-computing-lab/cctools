#ifndef WORK_QUEUE_JSON_H
#define WORK_QUEUE_JSON_H

#include "work_queue.h"

struct work_queue_task* work_queue_task_json_create(const char* str);
struct work_queue* work_queue_json_create(const char* str);
int work_queue_json_submit(struct work_queue *q, const char* str);
char* work_queue_json_wait(struct work_queue *q, int timeout);

#endif
