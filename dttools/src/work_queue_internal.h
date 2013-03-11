#include "work_queue.h"
#include "list.h"



struct work_queue_task *work_queue_wait_internal(struct work_queue *q, int timeout, struct list *aux_links, struct list *active_aux_links);


