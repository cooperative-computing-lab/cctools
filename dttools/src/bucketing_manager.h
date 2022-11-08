#ifndef BUCKETING_MANAGER_H
#define BUCKETING_MANAGER_H

#include "rmsummary.h"
#include "category.h"
#include "hash_table.h"
#include "bucketing_greedy.h"
#include "bucketing_exhaust.h"

/* A bucketing manager has its bucketing mode, a table mapping resource
 * type to its bucketing state, a table mapping task id to its latest
 * resource summary, and pointer to its task category */
typedef struct
{
    category_mode_t mode;   //bucketing mode in CATEGORY_ALLOCATION_MODE_{GREEDY/EXHAUSTIVE}_BUCKETING
    struct hash_table* res_type_to_bucketing_state; //mapping of resource type to bucketing state
    struct hash_table* task_id_to_task_rmsummary;   //mapping of task id to its previous resource summary
    category* category; //pointer to category, category and bucketing manager is one-to-one mapping
} 
bucketing_manager;

/* Create a bucketing manager
 * @param c the category of manager
 * @return pointer to a newly created bucketing manager
 * @return 0 if failure */
bucketing_manager* bucketing_manager_create(category* c);

/* Delete a bucketing manager
 * @param m the manager to be deleted
 * @return 0 if success
 * @return 1 if failure */
int bucketing_manager_delete(bucketing_manager* m);

/* Add a new type of resource to the manager
 * @param m the relevant manager
 * @param r the string of the resource (e.g., "cores")
 * @return 0 if success
 * @return 1 if failure */
int bucketing_manager_add_resource_type(bucketing_manager* m, const char* r);

/* Remove a type of resource from the manager
 * @param m the relevant manager
 * @param r the string of the resource (e.g., "cores")
 * @return 0 if success
 * @return 1 if failure */
int bucketing_manager_remove_resource_type(bucketing_manager* m, const char* r);

/* Change the bucketing algorithm of a manager
 * @param m the relevant manager
 * @param mode the mode of algorithm to change to
 * @return 0 if success
 * @return 1 if failure */
int bucketing_manager_change_mode(bucketing_manager* m, category_mode_t mode);

/* Given a task id, the manager returns a predicted allocation
 * @param m the relevant manager
 * @param task_id the task id
 * @return a pointer to a newly created rmsummary containing the prediction
 * @return 0 if failure */
rmsummary* bucketing_manager_alloc(bucketing_manager* m, int task_id);

/* Add a task's resource summary to the manager
 * @param m the relevant manager
 * @param r the resource summary of a task
 * @return 0 if success
 * @return 1 if failure */
int bucketing_manager_add(bucketing_manager* m, rmsummary* r);

#endif
