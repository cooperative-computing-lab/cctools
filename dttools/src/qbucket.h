#include <stdbool.h>
#include "int_sizes.h"

/* structure holding resources in interests of qbucket */
struct qbucket_resources
{
    double cores;
    double mem;
    double disk;
    double gpus;
    double sig;
};

/* QBucket_task structure, only track 1 type of resource per structure instance */
struct qbucket_task
{

    /* measured consumption of 1 resource */
    double measured_cons;

    /*significance value of current task */
    double sig;
};

/* QBucket structure */
struct qbucket
{

    /* The following arrays all have the same length */
    /* --------------------------------------------------- */
    
    /* a doubly linked list of qbucket_tasks of cores in sorted order */
    struct list *sorted_cores;

    /* a doubly linked list of qbucket_tasks of memory in sorted order */
    struct list *sorted_mem;
    
    /* a doubly linked list of qbucket_tasks of disk in sorted order */
    struct list *sorted_disk;
    
    /* a doubly linked list of qbucket_tasks of gpus in sorted order */
    struct list *sorted_gpus;
    
    /* a doubly linked list of indices to qbucket_tasks of cores
     * where each index is a bucket's delimiter */
    struct list *buckets_cores;

    /* a doubly linked list of indices to qbucket_tasks of memory
     * where each index is a bucket's delimiter */
    struct list *buckets_mem;

    /* a doubly linked list of indices to qbucket_tasks of disk
     * where each index is a bucket's delimiter */
    struct list *buckets_disk;

    /* a doubly linked list of indices to qbucket_tasks of gpus
     * where each index is a bucket's delimiter */
    struct list *buckets_gpus;
    
    /* ------------------------ */
    /* End of listing of arrays */

    /* default resource request, only use fields in {cores, memory, disk, gpus} */
    struct qbucket_resources *default_request;

    /* maximum resource request that a task can make, user specifies this */
    struct qbucket_resources *max_request;

    /* number of tasks needed to be run to warm up QBucket (fill it with some values)
    * -> cold start tasks */
    int64_t num_cold_start_tasks;
    
    /* total number of tasks completed without errors from WQ or resource exhaustion*/
    int64_t total_tasks;

    /* rate to increase resources when tasks fail in cold start phase or when 
     * tasks exceed all buckets in QBucket */
    double increase_rate;
    
    /* whether qbucket is in cold start phase, 1 is yes, 0 is no */
    int cold_start_phase;

    /* maximum number of tasks to keep in qbucket */
    int max_num_tasks;

    /* qbucket id */
    int qbucket_id;
};

/* Initialize default values for a qbucket_resources structure
 * @param qbres the pointer to a qbucket_resources structure
 * @param cores the number of cores
 * @param mem the number of memory in MBs
 * @param disk the number of disk in MBs
 * @param gpus the number of gpus
 */
void init_default_qbucket_resources(struct qbucket_resources *qbres, 
        double cores, double mem, double disk, double gpus, double sig);

/* Create a pointer to a qbucket struct
 * @return Pointer to a qbucket struct
 */
struct qbucket *qbucket_create();

/* Destroy qbucket struct and its elements
 * @param qb The qbucket to be destroyed
 */
void qbucket_destroy(struct qbucket *qb);

/* Initialize a qbucket
 * @param qbucket_id the id of the new qbucket
 * @return 1 if there's a problem
 * @return 0 if all is good
 */
int init_qbucket(int qbucket_id, struct qbucket *qb);

/* Create a qbucket_task structure
 * @return pointer to newly created qbucket_task structure
 */
struct qbucket_task *qbucket_task_create();

/* Destroy a qbucket_task struct
 * @param qbtask The pointer to a qbucket_task struct
 */
void qbucket_task_destroy(struct qbucket_task *qbtask);

/* Calculate the cost of partitioning at the current point
 * @param p1 Probability of lower half (including current point)
 * @param p2 Probability of upper half (excluding current point)
 * @param delim_res Resource value of current point
 * @param max_res Maximum resource value in the current range (low index to high index)
 * @param i Index of current point in sorted_res
 * @param low_index Low limit of range
 * @param high_index High limit of range
 * @param num_tasks_above_delim Number of tasks above delimiter/current point
 * @param sorted_res List of qbucket_task structures sorted in increasing order by measured_cons
 * @param bot_sig Array holding cumulative significances of points, have length of (high_index - low_index) 
 * @return the cost of partitioning at the current point
 */
static double __partitioning_policy(double p1, double p2, double delim_res, double max_res, 
        int i, int low_index, int high_index, int num_tasks_above_delim, 
        struct list *sorted_res, double *bot_sig);

/* Create a list of one element, which is a double
 * @param num The double to store
 * @return pointer to a list containing only one element
 */
static struct list *__create_one_num_list(double num);

/* Partitioning buckets into a list of indices to a list of resource values
 * @param sorted_res A list structure holding resource values in increasing order
 * @param low_index The low end of the current bucket
 * @param high_index The high end of the current bucket
 * @return A list containing indices that represent partitions of buckets in sorted_res
 */
static struct list *__bucket_partitioning(struct list *sorted_res, int low_index, int high_index);

/* Get an allocation for the current task with possible previous resource report
 * @param qb The current qbucket
 * @param res_type The resource type, can be {"cores", "mem", "disk", or "gpus"}
 * @param last_res The previous resource usage of asking task, if applicable
 * @param res_exceeded If previous attempt of running task exceeds resource allocation
 * @return New allocation value to try
 */
static double __get_allocation_resource(struct qbucket *qb, const char *res_type,
        double last_res, int res_exceeded);

/* Convert a qbucket_resources struct to a rmsummary struct
 * @param qbres The qbucket_resources needed to be converted
 * @return The rmsummary struct containing information from qbres
 */
struct rmsummary *qbucket_res_to_rmsummary(struct qbucket_resources *qbres);

/* A local comparing function output the minimum value of two parameters
 * @param a The first number
 * @param b The second number
 * @return min(a, b)
 */
static double min(double a, double b);

/* Create an empty qbucket_resources struct
 * @return A pointer to an empty qbucket_resources struct
 */
struct qbucket_resources *qbucket_resources_create();

/* Destroy a qbucket_resources struct
 * @param pbres The struct to be destroyed
 */ 
void qbucket_resources_destroy(struct qbucket_resources *qbres);

/* Get the allocation of the current task 
 * @param qb The relevant qbucket
 * @param task_prev_res The resource report of previous resource usage of current task
 * @param task_prev_alloc The resource report of previous allocation of current task
 * @return A rmsummary struct containing all relevant information to allocate tasks
 */
struct rmsummary *get_allocation(struct qbucket *qb, struct rmsummary *task_prev_res,
        struct rmsummary *task_prev_alloc);

/* Map a qbucket task to its priority in sorted resource list
 * @param qbtask The current qbtask
 * @return The qbtask's priority
 */
double qbucket_task_priority(struct qbucket_task *qbtask);

/* Add tasks to appropriate structures in a qbucket struct
 * @param qb The relevant qbucket struct
 * @param qbtask the qbucket_resources struct containing resource reports of a successfully completed task
 */

void add_task(struct qbucket *qb, struct qbucket_resources *qbtask);
