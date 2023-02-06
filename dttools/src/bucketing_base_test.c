#include <stdio.h>
#include <string.h>
#include "bucketing_greedy.h"
#include "bucketing_exhaust.h"
#include "bucketing.h"
#include "category.h"
#include "debug.h"

int main(int argc, char** argv)
{
    double default_value = 1000;
    int num_sampling_points = 10;
    double increase_rate = 2;
    int max_num_buckets = 10;
    int update_epoch = 1;

    bucketing_mode_t mode;
    if (argc == 2)
    {
        if (strncmp(*(argv+1), "-greedy", 7) == 0)
        {
            mode = BUCKETING_MODE_GREEDY;
        }
        else if (strncmp(*(argv+1), "-exhaust", 8) == 0)
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
    
    printf("Creating bucketing state\n");
    bucketing_state_t* s = bucketing_state_create(default_value, num_sampling_points, increase_rate, max_num_buckets, mode, update_epoch);

    int prime = 7000;
    int num = 2000;
    int multiple = 2;
    int iters = 50;
    double pred;
    double prev_val = -1;
    
    double avg_eff = 0;

    double alloc = 0;

    printf("Adding values\n");
    for (int i = 0; i < iters; ++i)
    {
        num = num * multiple % prime;
        bucketing_sorted_points_print(s->sorted_points);
        bucketing_sorted_buckets_print(s->sorted_buckets);
        printf("iteration %d data value %d\n", i, num);
        while ((pred = bucketing_predict(s, prev_val)))
        {
            alloc += pred;
            printf("predict %lf\n", pred);
            if (pred < num)
            {
                prev_val = pred;
            }
            else
            {
                prev_val = -1;
                break;
            }
        }

        if (alloc > 0)
            avg_eff += num/alloc;
        printf("average efficiency so far %lf\n", avg_eff/(i+1));

        bucketing_add(s, num);
        alloc = 0;
        //printf("value added\n");
        //if (i >= num_sampling_points - 1)
        //{
            //printf("Finding buckets\n");
        //    bucketing_greedy_update_buckets(s);
        //}
        //printf("Sorted list length %d\n", list_length(s->sorted_points));
        printf("----------------------------------\n");
    }
    bucketing_state_delete(s);
    return 0;
}
