#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "bucketing_manager.h"
#include "debug.h"
#include "twister.h"

extern struct hash_table* info_of_resource_table;

int main(int argc, char** argv)
{
    bucketing_mode_t mode;
    if (argc == 2)
    {
        if (strncmp(*(argv+1), "-greedy", 7) == 0)
            mode = BUCKETING_MODE_GREEDY;
        else if (strncmp(*(argv+1), "-exhaust", 8) == 0)
            mode = BUCKETING_MODE_EXHAUSTIVE;
        else
        {
            fatal("invalid bucketing mode\n");
            return 1;
        }
    }
    else
    {
        fatal("must specify bucketing mode\n");
        return 1;
    }
    double default_value;
    int num_sampling_points = 10;
    double increase_rate = 2;
    int max_num_buckets = 10;
    int update_epoch = 1;
    int set_default = 0;

    twister_init_genrand64(15112022);

    bucketing_manager_t* m = bucketing_manager_create(mode);
    char* res_names[3] = {"cores", "memory", "disk"};

    for (int i = 0; i < 3; ++i)
    {
        default_value = i == 0 ? 1 : 1000;
        bucketing_manager_add_resource_type(m, res_names[i], set_default, default_value, num_sampling_points, increase_rate, max_num_buckets, update_epoch);
    }

    int prime_mem = 7000;
    int num_mem = 2000;
    int prime_disk = 7000;
    int num_disk = 2000;
    int prime_core = 7;
    int num_core = 2;
    int multiple = 2;
    int iters = 50;

    struct rmsummary* task_r;
    struct rmsummary* pred_task_r;
    
    double eff_core = 0;
    double eff_mem = 0;
    double eff_disk = 0;

    double alloc_core = 0;
    double alloc_mem = 0;
    double alloc_disk = 0;

    int task_id = 1;

    //printf("Adding values\n");
    for (int i = 0; i < iters; ++i)
    {
        task_r = rmsummary_create(-1);
        num_core = num_core * multiple % prime_core;
        num_mem = num_mem * multiple % prime_mem;
        num_disk = num_disk * multiple % prime_disk;
        rmsummary_set(task_r, res_names[0], num_core);
        rmsummary_set(task_r, res_names[1], num_mem);
        rmsummary_set(task_r, res_names[2], num_disk);
        printf("iteration %d task w cores %d mem %d disk %d\n", i+1, num_core, num_mem, num_disk);

        struct hash_table* tmp_ht = m->res_type_to_bucketing_state;
        char* tmp_name;
        bucketing_state_t* tmp_state;
        hash_table_firstkey(tmp_ht);
        while(hash_table_nextkey(tmp_ht, &tmp_name, (void**) &tmp_state))
        {
            printf("buckets for %s\n", tmp_name);
            bucketing_sorted_buckets_print(tmp_state->sorted_buckets);
        }

        while((pred_task_r = bucketing_manager_predict(m, task_id)))
        {
            printf("prediction: cores %lf mem %lf disk %lf\n", pred_task_r->cores, pred_task_r->memory, pred_task_r->disk);
            alloc_core += pred_task_r->cores;
            alloc_mem += pred_task_r->memory;
            alloc_disk += pred_task_r->disk;

            if (pred_task_r->cores >= task_r->cores && pred_task_r->memory >= task_r->memory && pred_task_r->disk >= task_r->disk)
            {
                bucketing_manager_add_resource_report(m, task_id, task_r, 1);
                rmsummary_delete(pred_task_r);
                break;
            }
            else
            {
                if (!pred_task_r->limits_exceeded)
                {
                    pred_task_r->limits_exceeded = rmsummary_create(-1);
                    if (pred_task_r->cores < task_r->cores)
                        pred_task_r->limits_exceeded->cores = 1;
                    if (pred_task_r->memory < task_r->memory)
                        pred_task_r->limits_exceeded->memory = 1;
                    if (pred_task_r->disk < task_r->disk)
                        pred_task_r->limits_exceeded->disk = 1;
                }
                bucketing_manager_add_resource_report(m, task_id, pred_task_r, 0);
                rmsummary_delete(pred_task_r);
            }
        }
        eff_core += task_r->cores/alloc_core;
        eff_mem += task_r->memory/alloc_mem;
        eff_disk += task_r->disk/alloc_disk;
        rmsummary_delete(task_r);
        printf("efficiency core %.3lf mem %.3lf disk %.3lf\n so far", eff_core/(i+1), eff_mem/(i+1), eff_disk/(i+1));
        alloc_core = 0;
        alloc_mem = 0;
        alloc_disk = 0;
        ++task_id;
        printf("----------------------------------\n");
    }
    bucketing_manager_delete(m);
    hash_table_delete(info_of_resource_table);
    return 0;
}
