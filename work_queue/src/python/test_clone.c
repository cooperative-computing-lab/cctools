#include "work_queue.h"
#include "debug.h"
#include <stdio.h>

int main(int argc, char *argv[]) {
  debug_flags_set("all");

  struct work_queue *q = work_queue_create(9123);
  struct work_queue_task
    *t0,
    *t1,
    *t2,
    *t3;

  t0 = work_queue_task_create("cat input.txt >output.txt");
  work_queue_task_specify_file(t0, argv[0]     , "input.txt" , WORK_QUEUE_INPUT , WORK_QUEUE_NOCACHE);
  work_queue_task_specify_file(t0, "output.txt", "output.txt", WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE);

  t1 = work_queue_task_clone(t0);
  t2 = work_queue_task_clone(t0);
  t3 = work_queue_task_clone(t1);

  work_queue_submit(q, t0);
  printf("submitted %d\n", (int)t0);
  work_queue_submit(q, t1);
  printf("submitted %d\n", (int)t1);
  work_queue_submit(q, t2);
  printf("submitted %d\n", (int)t2);
  work_queue_submit(q, t3);
  printf("submitted %d\n", (int)t3);

  while (!work_queue_empty(q)) {
    struct work_queue_task *r = work_queue_wait(q, 5);
    if (r) {
      printf("%d\n", (int)r);
    }
  }

  return 0;
}
