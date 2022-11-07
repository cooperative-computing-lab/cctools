#include <stdio.h>
#include <math.h>
#include "bucketing_exhaust.h"
#include "bucketing.h"

int main()
{
    double default_value = 1000;
    int num_sampling_points = 10;
    double increase_rate = 2;

    //printf("Creating bucketing state\n");
    bucketing_state* s = bucketing_state_create(default_value, num_sampling_points, increase_rate, 10);

    int prime = 7000;
    int num = 2000;
    int multiple = 2;
    int iters = 50;

    //printf("Adding values\n");
    for (int i = 0; i < iters+20; ++i)
    {
        if (i >= iters)
            num = 10;
        else
            num = num * multiple % prime;
        //printf("iteration %d data value %d num points %d\n", i, num, list_size(s->sorted_points));
        if (bucketing_add(num, i + 1, s))
            return 1;
        //printf("value added\n");
        //bucketing_sorted_points_print(s->sorted_points);
        if (i >= num_sampling_points - 1)
        {
            //printf("Finding buckets\n");
            bucketing_exhaust_update_buckets(s);
            //bucketing_sorted_buckets_print(s->sorted_buckets);
        }
        //printf("Predicting value %lf\n", bucketing_predict(-1, s));
        //printf("Sorted list length %d\n", list_length(s->sorted_points));
        //printf("----------------------------------\n");
    }
    bucketing_state_delete(s);
    return 0;
}
