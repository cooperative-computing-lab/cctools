#include "bucketing_manager.c"

bucketing_manager* bucket_manager_create(category* c){
    bucketing_manager* m = malloc(sizeof(*m));
    if (!m)
        return 0;

    m->mode = c->allocation_mode;
    m->res_type_to_state = hash_table_create(0, 0);
    m->task_id_to_task_rmsummary = hash_table_create(0, 0);
    m->category = category;


}

//takes in a category c. c->autolabel_resource->mem = {0, 1}
void add(category* c);

void predict(category* c);
