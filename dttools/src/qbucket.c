/** @file qbucket.c Implements the QBucket algorithm to allocate resources to tasks.
 * Each task's category has its own QBucket object
 */

/* QBucket structure */
struct qbucket {

    /*qbucket id*/
    int qbucket_id;

    /* number of tasks needed to be run to warm up QBucket (fill it with some values)
    * -> cold start tasks*/
    int64_t num_cold_start_tasks;
    
    /* rate to increase resources when tasks fail in cold start phase or when 
     * tasks exceed all buckets in QBucket */
    int increase_rate;
    
    /* default resource request, only use fields in {cores, memory, disk, gpus} */
    struct rmsummary *default_request;

    /* total number of tasks completed without errors from WQ or resource exhaustion*/
    int64_t total_tasks;
}
