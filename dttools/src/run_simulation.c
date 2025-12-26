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
    const char* det_greedy_str = "-det-greedy";
    const char* greedy_str = "-greedy";
    const char* exhaust_str = "-exhaust";
    const char* det_exhaust_str = "-det-exhaust";

    if (argc == 2)
    {
        if (strncmp(*(argv+1), det_greedy_str, strlen(det_greedy_str)) == 0)
        {
            mode = BUCKETING_MODE_DET_GREEDY;
        }
        else if (strncmp(*(argv+1), det_exhaust_str, strlen(det_exhaust_str)) == 0)
        {
            mode = BUCKETING_MODE_DET_EXHAUSTIVE;
        }
        else if (strncmp(*(argv+1), greedy_str, strlen(greedy_str)) == 0)
        {
            mode = BUCKETING_MODE_GREEDY;
        }
        else if (strncmp(*(argv+1), exhaust_str, strlen(exhaust_str)) == 0)
        {
            mode = BUCKETING_MODE_EXHAUSTIVE;
        }
        else
        {
            fatal("Invalid bucketing mode\n");
            return 1;
        }    
    }
    else
    {
        fatal("Must provide type of bucketing mode\n");
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

    FILE *file = fopen("../../../topeft/simulated.data", "r");
    char buffer[256];
    double task_id, wall_time, cores, mem, disk;
    struct rmsummary* task_r;
    struct rmsummary* pred_task_r;

    double total_core_allocated_workflow = 0;
    double total_mem_allocated_workflow = 0;
    double total_disk_allocated_workflow = 0;

    double total_core_real_consumed_workflow = 0;
    double total_mem_real_consumed_workflow = 0;
    double total_disk_real_consumed_workflow = 0;

    double total_core_internal_fragment_workflow = 0;
    double total_mem_internal_fragment_workflow = 0;
    double total_disk_internal_fragment_workflow = 0;

    // skip first line
    fgets(buffer, sizeof(buffer), file);

    int iter = 0;
    while (fgets(buffer, sizeof(buffer), file)) {
        ++iter;
        if (iter == 5000){
            exit(1);
        }

        int matched = sscanf(buffer, "%lf %lf %lf %lf %lf", &task_id, &wall_time, &cores, &mem, &disk);

        //printf("task id %lf\n", task_id);
        
        if (total_core_allocated_workflow != 0) {
            printf("global core efficiency: %lf\n", total_core_real_consumed_workflow/total_core_allocated_workflow);
            printf("global mem efficiency: %lf\n", total_mem_real_consumed_workflow/total_mem_allocated_workflow);
            printf("global disk efficiency: %lf\n", total_disk_real_consumed_workflow/total_disk_allocated_workflow);
        }


        if (matched != 5) {
            printf("Failed!");
            return 1;          
        }
       
        task_r = rmsummary_create(-1);
        rmsummary_set(task_r, res_names[0], cores);
        rmsummary_set(task_r, res_names[1], mem);
        rmsummary_set(task_r, res_names[2], disk);
        printf(" task id %lf wall time %lf cores %lf mem %lf disk %lf\n", task_id, wall_time, cores, mem, disk);

        struct hash_table* tmp_ht = m->res_type_to_bucketing_state;
        char* tmp_name;
        bucketing_state_t* tmp_state;
        hash_table_firstkey(tmp_ht);
        while(hash_table_nextkey(tmp_ht, &tmp_name, (void**) &tmp_state))
        {
            printf("buckets for %s\n", tmp_name);
            bucketing_sorted_buckets_print(tmp_state->sorted_buckets);
        }
        int predict_times = 0;
        while((pred_task_r = bucketing_manager_predict(m, task_id)))
        {
            ++predict_times;
            printf("prediction: cores %lf mem %lf disk %lf\n", pred_task_r->cores, pred_task_r->memory, pred_task_r->disk);
            total_core_allocated_workflow += pred_task_r->cores * wall_time;
            total_mem_allocated_workflow += pred_task_r->memory * wall_time;
            total_disk_allocated_workflow += pred_task_r->disk * wall_time;

            // avoid double precision calculation stuff
            // 1 is to avoid equality
            int core_diff = pred_task_r->cores*1000 - task_r->cores*1000 + 1;
            int mem_diff = pred_task_r->memory*1000 - task_r->memory*1000 + 1;
            int disk_diff = pred_task_r->disk*1000 - task_r->disk*1000 + 1;

            if (core_diff > 0 && mem_diff > 0 && disk_diff > 0)
            {
                bucketing_manager_add_resource_report(m, task_id, task_r, 1);
                total_core_real_consumed_workflow += task_r->cores * wall_time;
                total_mem_real_consumed_workflow += task_r->memory * wall_time;
                total_disk_real_consumed_workflow += task_r->disk * wall_time;
                total_core_internal_fragment_workflow += (pred_task_r->cores-task_r->cores) * wall_time;
                total_mem_internal_fragment_workflow += (pred_task_r->memory-task_r->memory) * wall_time;
                total_disk_internal_fragment_workflow += (pred_task_r->disk-task_r->disk) * wall_time;
                rmsummary_delete(pred_task_r);
                break;
            }
            else
            {
                if (!pred_task_r->limits_exceeded)
                {
                    pred_task_r->limits_exceeded = rmsummary_create(-1);
                    if (pred_task_r->cores < task_r->cores) {
                        pred_task_r->limits_exceeded->cores = 1;
                        pred_task_r->cores = task_r->cores;
                    }
                    if (pred_task_r->memory < task_r->memory) {
                        pred_task_r->limits_exceeded->memory = 1;
                        pred_task_r->memory = task_r->memory;
                    }
                    if (pred_task_r->disk < task_r->disk) {
                        pred_task_r->limits_exceeded->disk = 1;
                        pred_task_r->disk = task_r->disk;
                    }
                }
                bucketing_manager_add_resource_report(m, task_id, pred_task_r, 0);
                rmsummary_delete(pred_task_r);
            }
        }
        printf("Predict times is %d\n", predict_times);
        predict_times = 0;
        rmsummary_delete(task_r);
        //printf("----------------------------------\n");
    }
    bucketing_manager_delete(m);
    hash_table_delete(info_of_resource_table);

    printf("global core efficiency: %lf\n", total_core_real_consumed_workflow/total_core_allocated_workflow);
    printf("global mem efficiency: %lf\n", total_mem_real_consumed_workflow/total_mem_allocated_workflow);
    printf("global disk efficiency: %lf\n", total_disk_real_consumed_workflow/total_disk_allocated_workflow);

    return 0;
}
