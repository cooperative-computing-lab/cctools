#include <stdio.h>

#include "work_queue.h"
#include "timestamp.h"

int main(int argc, char **argv) {
    struct work_queue *q = work_queue_create(9123);

    struct work_queue_task *t_vanilla = work_queue_task_create("python --version");
    work_queue_task_specify_tag(t_vanilla, "vanilla");

    struct work_queue_task *t_conda   = work_queue_task_create("python --version");
    work_queue_task_specify_tag(t_conda, "with conda");
    work_queue_task_specify_conda_env(t_conda, "conda-coffea-wq-env-py3.8.tar.gz");

    struct work_queue_task *t_conda_b   = work_queue_task_create("python --version");
    work_queue_task_specify_tag(t_conda_b, "with conda");
    work_queue_task_specify_conda_env(t_conda_b, "conda-coffea-wq-env-py3.8.tar.gz");

    work_queue_submit(q, t_vanilla);
    work_queue_submit(q, t_conda);
    work_queue_submit(q, t_conda_b);

    timestamp_t origin = timestamp_get();

    while(!work_queue_empty(q)) {
        struct work_queue_task *t = work_queue_wait(q, -1);
        if(t) {
            printf("%lf task %s output: %s\n", (timestamp_get() - origin)/1e6, t->tag, t->output);
            work_queue_task_delete(t);
        }
    }

    work_queue_delete(q);
}
