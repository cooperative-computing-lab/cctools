/*
   Copyright (C) 2016- The University of Notre Dame
   This software is distributed under the GNU General Public License.
   See the file COPYING for details.
   */

#include <stdio.h>
#include <string.h>

#include "category_internal.h"
#include "rmsummary.h"
#include "debug.h"

const char *category = "test";


void print_times(struct category *c) {
    struct histogram *h = c->disk_histogram;
    int64_t n = histogram_size(h);

    double *keys = histogram_buckets(h);

    double tau_mean;
    double *counts_cdp  = malloc(n*sizeof(double));
    double *times_accum = malloc(n*sizeof(double));

    category_first_allocation_accum_times(h, keys, &tau_mean, counts_cdp, times_accum);    

    fprintf(stdout, "%-6s %-8s %-8s %-16s %-16s %-16s\n", "alloc", "count", "cdp", "times_acc", "W (min best)", "T (max best)");

    int i;
    for(i = 0; i < n; i++) {

        double a  = keys[i];
        double a_m = keys[n-1];

        int count = histogram_count(h, a);

        double Ea = a*tau_mean + a_m*times_accum[i];

        double Pbef = counts_cdp[i];
        double Paft = 1 - Pbef;

        double numerator   = (Pbef*a_m)/a + Paft;
        double denominator = tau_mean + times_accum[i];

        double  Ta = numerator/denominator;

        fprintf(stdout, "%6.0lf %8d %8.5lf %16.6lf %16.6lf %12.6lf \n", a, count, counts_cdp[i], times_accum[i], Ea, Ta);
    }

    return;
}


int main(int argc, char **argv) {

    const char *input_name = argv[1];

    FILE *input_f = fopen(input_name, "r");
    if(!input_f) {
        fatal("Could not open '%s'", input_name);
    }

    double wall_time;
    char   state[64], id[64];
    int    disk;

    struct hash_table *cs = hash_table_create(0, 0);
    struct category *c = category_lookup_or_create(cs, category);

    category_specify_allocation_mode(c, CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT);

    while(fscanf(input_f, "%s %s %lf %d", id, state, &wall_time, &disk) == 4) {
        struct rmsummary *s = rmsummary_create(-1);

        s->category  = strdup(category);
        s->taskid    = strdup(id);
        s->disk      = disk;
        s->wall_time = wall_time;

        if(strcmp(state, "SUCCESS") != 0) {
            continue;
        }

        category_accumulate_summary(c, s, NULL);
    }

    category_update_first_allocation(c, NULL);

    if(c->first_allocation) {
        print_times(c);
        fprintf(stdout, "%" PRId64 "\n", c->first_allocation->disk);
    }
}

