#ifndef BUCKETING_MANAGER_H
#define BUCKETING_MANAGER_H

#include "rmsummary.h"
#include "category.h"
#include "hash_table.h"
#include "bucketing_greedy.h"
#include "bucketing_exhaust.h"

/* A bucketing manager has its bucketing mode, a table mapping resource
 * type to its bucketing state, and a table mapping task id to its latest
 * resource summary */
typedef struct
{
    category_mode_t mode;   //bucketing mode in CATEGORY_ALLOCATION_MODE_{GREEDY/EXHAUSTIVE}_BUCKETING
    struct hash_table* res_type_to_bucketing_state; //mapping of resource type to bucketing state
    struct hash_table* task_id_to_task_rmsummary;   //mapping of task id to its previous resource summary from either actual run or prediction
} 
bucketing_manager_t;

/* Create a bucketing manager
 * @param mode category mode of manager
 * @return pointer to a newly created bucketing manager */
bucketing_manager_t* bucketing_manager_create(category_mode_t mode);

/* Delete a bucketing manager
 * @param m the manager to be deleted */
void bucketing_manager_delete(bucketing_manager_t* m);

/* Add a new type of resource to the manager
 * Do nothing if this resource is already in manager
 * @param m the relevant manager
 * @param r the string of the resource (e.g., "cores")
 * @param default_value the first base value to allocate new tasks (e.g.,"mem": 1000 means try 1GBs of mem to new tasks)
 * @param num_sampling_points number of sampling points
 * @param increase_rate the rate to increase value when task fails in sampling phase or when task consumes more than any other tasks in predicting phase
 * @param max_num_buckets the maximum number of buckets to try to break (only for EXHAUSTIVE_BUCKETING) */
void bucketing_manager_add_resource_type(bucketing_manager_t* m, const char* r, double default_value, int num_sampling_points, double increase_rate, int max_num_buckets);

/* Remove a type of resource from the manager
 * Do nothing if this resource is not in manager
 * @param m the relevant manager
 * @param r the string of the resource (e.g., "cores") */
void bucketing_manager_remove_resource_type(bucketing_manager_t* m, const char* r);

/* Set the bucketing algorithm of a manager
 * @param m the relevant manager
 * @param mode the mode of algorithm to change to */
void bucketing_manager_set_mode(bucketing_manager_t* m, category_mode_t mode);

/* Given a task id, the manager returns a predicted allocation and adds this prediction into internal state. The caller is responsible for free'ing the returned value.
 * @param m the relevant manager
 * @param task_id the task id
 * @return a pointer to a newly created rmsummary containing the prediction
 * this rmsummary's entries are 0 if no prediction, otherwise entries are predicted
 * @return 0 if failure */
struct rmsummary* bucketing_manager_predict(bucketing_manager_t* m, int task_id);

/* Add a task's resource summary to the manager. The caller is responsible for free'ing the parameter r after calling this function.
 * @param m the relevant manager
 * @param task_id task id of task to be added
 * @param r the resource summary of a task
 * @param success whether task succeeds in running or not (i.e., task doesn't exceed resource limits) */
void bucketing_manager_add_resource_report(bucketing_manager_t* m, int task_id, struct rmsummary* r, int success);

#endif
