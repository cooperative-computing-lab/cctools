#ifndef WORK_QUEUE_JSON_H
#define WORK_QUEUE_JSON_H

int is_in(const char* str, char* a[]);

int validate_json(struct jx *json, char** a);

int specify_files(int input, struct jx *files, struct work_queue_task *task);

struct work_queue_task* work_queue_task_json_create(char* str);

struct work_queue* work_queue_json_create(char* str);

int work_queue_json_submit(struct work_queue *q, char* str);

#endif
