/*
 Copyright (C) 2008- The University of Notre Dame
 This software is distributed under the GNU General Public License.
 See the file COPYING for details.
 */

#ifndef MAKEFLOW_HOOK_H_
#define MAKEFLOW_HOOK_H_

/** @file
 *
 * This file defines the library interface of MAKEFLOW HOOK
 *
 */

#include "batch_job.h"
#include "batch_task.h"
#include "dag.h"
#include "dag_node.h"
#include "dag_file.h"
#include "jx.h"

/* ----------------------------------------------------------- *
 * Basic MAKEFLOW HOOK API					       *
 * ----------------------------------------------------------- */

/**
 * Configuration example of the high-level API
 *
 * The first step is to write the function definitions that will
 * be specified in the hook struct.
 *
 * E.G.
 * int makeflow_hook_example_create(struct jx *jx){
 *		printf("Hello from module: EXAMPLE.\n");
 *		return MAKEFLOW_HOOK_SUCCESS;
 * }
 *
 * NOTE: Unless specified in the documentation, a return of 
 * MAKEFLOW_HOOK_SUCCESS indicates success, and other return
 * codes indicate varies failures. NO FURTHER ACTION IS TAKEN 
 * ON FAILURE, unless specified. Makeflow/workflow will abort.
 * Return MAKEFLOW_HOOK_SUCCESS unless fatal error.
 * 
 * The second step for utilizing this HOOK API is to create a
 * uniquely named struct for your HOOK. This struct will be used
 * to equate (using function pointers) a function definition with
 * the events in Makeflow's event loop. This struct should be 
 * located at the bottom of your module definition, and visible
 * from other compilation units, i.e. not static.
 *
 * E.G.
 * struct makeflow_hook makeflow_hook_example = {
 *    .module_name = "EXAMPLE",
 *    .create = makeflow_hook_example_create,
 *    .destroy = makeflow_hook_example_destroy,
 * }
 *
 * NOTE: It is important to only specify event hooks that are
 * defined. This will allow for flexible addition of hooks in
 * the future without breaking existing functionality. 
 * -- C99 Feature
 *
 * The third step is to add and register your hook in Makeflow.
 *
 * E.G.
 * extern struct makeflow_hook makeflow_hook_example;
 * makeflow_hook_register_hook(&makeflow_hook_example);
 *
 * Finally, the file where your hook definition resides needs
 * to be added to the makeflow/src/Makefile so that it is built.
 *
 */

/**
 * The makeflow hook operations:
*
 * Each hook operations corresponds to a transition in the
 * state of a structure in Makeflow. The three main structures
 * that hold state in Makeflow are the DAG, nodes, and files.
 * 
 * DAG: UNINIT -> PARSE -> START -> [END, FAILED, ABORTED]
 * NODE: -> CREATED -> WAITING -> RUNNING -> [COMPLETE, FAILED, ABORTED]
 * FILE: CREATE -> EXPECT -> EXIST -> COMPLETE -> CLEAN -> DELETED
 *             
 *
 * All methods are optional. 
 *
 * Here are list the immutable features and structures for each
 * high level struct:
 * DAG: parse, check, start, loop, end, fail, abort
 * Node: create, check, submit, end, fail, success
 * File: complete, clean, delete
 */
struct makeflow_hook {

	/* Module Name.
	 *
	 * MUST BE DEFINED.
	 *
	 * This name defines how we can identify this module.
	 * This is used in register hook and to identify 
	 * debug and failure statements.
	 */
	const char * module_name;
	struct jx * args;
	void * instance_struct;

	/* Register hook.
	 *
	 * Registers the hook into a linked list of hooks. The order of the
	 * hooks may vary with the order of invocation from the argument
	 * parsing in makeflow. This hook allows a the called hook to inspect
	 * the registered hooks and determine:
	 * 1) If it can be added (we can disallow conflicting hooks).
	 * 2) If multiple instance can be used, such as nesting containers or
	 *		wrappers.
	 * 3) If features are affected by presence of other hooks
	 *		(allowance of absolute paths).
	 *
	 * @param hook The hook that is being registered.
	 * @param hook_list The list of already registered hooks.
	 * @param args The JX struct that is used in calling env. Used to pass back
	 *      unique args struct for each instance of hook.
	 * @return MAKEFLOW_HOOK_SUCCESS if it is to be added, 
	 *		and MAKEFLOW_HOOK_SKIP if it is to be skipped.
	 */
	int (*register_hook) (struct makeflow_hook *hook, struct list *hook_list, struct jx **args);

	/* Initialize hooks.
	 *
	 * Initiallizing call to a hook. All arguments that are needed by the 
	 * hook should be added to a jx struct that is passed in to all create
	 * calls. This will allow for a variable number and set of arguments to
	 * a varying number of hooks.
	 *
	 * @param jx the struct with arguments for hook.
	 * @return MAKEFLOW_HOOK_SUCCESS if successfully created, MAKEFLOW_HOOK_FAILURE if failed.
	 */
	int (*create)        (void ** instance_struct, struct jx *hook_args);

	/* Destroy/Clean up hooks.
	 *
	 * Call when Makeflow is about to exit. This is to clean up any memory
	 * or structs left during execution. Most will be removed when exiting.
	 * This should only be used to clean up internal strucutures.
	 *
	 * @param d The DAG that is about to be destroyed.
	 * @return MAKEFLOW_HOOK_SUCCESS if successfully destroyed, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*destroy)       (void * instance_struct, struct dag *d);

	/* Hook after to dag validation.
	 * 
	 * This is set after dag parse, but prior to DAG start.
	 * This is meant for hooks for fail out on impossible configurations.
	 * I.E. remote names when using sharedfs.
	 * This event occurs prior to DAG_CLEAN. CLEAN will terminate
	 * Makeflow after this point.
	 *
	 * @return MAKEFLOW_HOOK_SUCCESS if dag check step successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*dag_check)      (void * instance_struct, struct dag *d);

	/* Hook into dag clean, after parsing.
	 * 
	 * This hook is inside of the clean mode check.
	 * Meant for deleting files and cleaning up loose ends not managed by Makeflow.
	 * Makeflow should handle file clean up, but in the case of Mount this may 
	 * clean up mount files and directory out of Makeflow's control.
	 *
	 * @param dag The DAG about to be started.
	 * @return MAKEFLOW_HOOK_SUCCESS if dag start step successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*dag_clean)     (void * instance_struct, struct dag *d);


	/* Hook prior to dag start, but after parsing.
	 * 
	 * This is used to augment the DAG or utilize information from the
	 * DAG to make decisions. An example of this is the storage
	 * allocation module that looks at file information that is parsed.
	 * This event occurs after DAG_CLEAN. CLEAN will terminate Makeflow 
	 * before this point.
	 *
	 * @param dag The DAG about to be started.
	 * @return MAKEFLOW_HOOK_SUCCESS if dag start step successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*dag_start)     (void * instance_struct, struct dag *d);

	/* Hook that determines if main event look should continue.
	 * 
	 * This is a hook that allows for you to continue running the Makeflow
	 * even if jobs were not added to an active queue. This is of use for
	 * systems such as archiving that may retrieve completed jobs.
	 *
	 * @param dag The DAG that is looping.
	 * @return MAKEFLOW_HOOK_SUCCESS to continue looping, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*dag_loop)     (void * instance_struct, struct dag *d);

	/* Hook for a completed DAG.
	 *
	 * Failing a hook at this point indicates that work was left undone or
	 * failed as a result of the hook. This is used to check if the DAG was 
	 * left in an unfinished state as a result of the hook, which is possible
	 * when using resource allocations.
	 *
	 * MAKEFLOW_HOOK_FAILURE will cause Makeflow to exit with a failed status.
	 *
	 * @param dag The DAG that was complete.
	 * @return MAKEFLOW_HOOK_SUCCESS if dag end step successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*dag_end)       (void * instance_struct, struct dag *d);

	/* Hook for a failed DAG.
	 * 
	 * This does not change that the DAG has failed, but gives the
	 * hook access to internal stats for failure analysis.
	 *
	 * @param dag The DAG that was failed.
	 * @return MAKEFLOW_HOOK_SUCCESS if dag fail step successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*dag_fail)      (void * instance_struct, struct dag *d);

	/* Hook for an aborted DAG.
	 * 
	 * This does not change that the DAG has aborted, but gives the
	 * hook access to internal stats for abort analysis.
	 *
	 * @param dag The DAG that was aborted.
	 * @return MAKEFLOW_HOOK_SUCCESS if dag abort step successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*dag_abort)     (void * instance_struct, struct dag *d);

	/* Hook for a successfully completed DAG.
	 * 
	 * This does not change that the DAG has success, but gives the
	 * hook access to internal stats for success analysis.
	 *
	 * @param dag The DAG that was aborted.
	 * @return MAKEFLOW_HOOK_SUCCESS if dag abort step successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*dag_success)     (void * instance_struct, struct dag *d);

	/* Hook when a node is checked for submission.
	 * 
	 * This hook occurs when nodes are checked for execution. This
	 * allows hooks to veto submission based on internal qualifiers.
	 * Example would be the storage allocation, or job limits.
	 *
	 * @param dag_node The dag_node that is being checked.
	 * @param queue The batch_queue being submitted to.
	 * @return MAKEFLOW_HOOK_SUCCESS if successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*node_check)    (void * instance_struct, struct dag_node *node, struct batch_queue *queue);

	/* Hook just prior to node submission.
	 * 
	 * This hook occurs just before nodes are executed. This
	 * allows hooks augment submission within the Makeflow context.
	 *
	 * This is the correct location to `wrap` tasks using a wrapper for
	 * execution.
	 *
	 * @param dag_node The dag_node that is being submitted.
	 * @param task The task being submitted.
	 * @return MAKEFLOW_HOOK_SUCCESS if successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*node_submit)   (void * instance_struct, struct dag_node *node, struct batch_task *task);

	/* Hook after node is collected, but prior to qualifying node success.
	 * 
	 * This hook occurs after node is retrieved from batch queue. This
	 * allows hooks to perform on outputted files and see exit status.
	 *
	 * @param dag_node The dag_node that was collected.
	 * @param task The task being submitted.
	 * @return MAKEFLOW_HOOK_SUCCESS if successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*node_end)      (void * instance_struct, struct dag_node *node, struct batch_task *task);

	/* Hook if node was successful.
	 * 
	 * @param dag_node The dag_node that was successful.
	 * @param task The task that succeeded.
	 * @return MAKEFLOW_HOOK_SUCCESS if successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*node_success)  (void * instance_struct, struct dag_node *node, struct batch_task *task);

	/* Hook if node failed.
	 * 
	 * @param dag_node The dag_node that failed.
	 * @param task The task that failed.
	 * @return MAKEFLOW_HOOK_SUCCESS if successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*node_fail)     (void * instance_struct, struct dag_node *node, struct batch_task *task);

	/* Hook if node aborted.
	 * 
	 * @param dag_node The dag_node that was aborted.
	 * @return MAKEFLOW_HOOK_SUCCESS if successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*node_abort)    (void * instance_struct, struct dag_node *node);

	/* Augment/Modify the job structure passed to batch system.
	 *
	 * The intended use case of this is for performing batch specific
	 * modifications, such as sharedfs. Sharedfs is the example as 
	 * it does not change the actual structure of a job, but is the
	 * oppurtunity to change what is passed to the batch system. Files
	 * `forgotten` by sharedfs are not forgotten in Makeflow.
	 *
	 * The batch_task contains the job to be passed
	 * to allow for modifications of the structure.
	 *
	 * @param task The task being submitted.
	 * @return MAKEFLOW_HOOK_SUCCESS if successfully modified, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*batch_submit) ( void * instance_struct, struct batch_task *task);

	/* Fix/Augment/Modify the job structure retrieved from batch system.
	 *
	 * The intended use case of this is for performing batch specific
	 * modifications, such as sharedfs. Sharedfs is the example as 
	 * it does not change the actual structure of a job, but is the
	 * oppurtunity to change what is passed to the batch system. Files
	 * `forgotten` by sharedfs will be added back in so they 
	 * are not forgotten in Makeflow.
	 *
	 * @param task The task retrieved from queue.
	 * @return MAKEFLOW_HOOK_SUCCESS if successfully modified, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*batch_retrieve) ( void * instance_struct, struct batch_task *task);

	/* Hook when file is registered as complete.
	 *
	 * Complete means that the file still exists, 
	 * but is no longer used. The next step is to 
	 * clean it. Used in node_complete 
	 *
	 * @param dag_file The dag_file that completed.
	 * @return MAKEFLOW_HOOK_SUCCESS is successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*file_complete) (void * instance_struct, struct dag_file *file);

	/* Hook when file is about to be clean.
	 *
	 * This is used for archiving files are storing files prior
	 * to removal.
	 *
	 * @param dag_file The dag_file that is to be cleaned.
	 * @return MAKEFLOW_HOOK_SUCCESS is successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*file_clean)    (void * instance_struct, struct dag_file *file);

	/* Hook when file has been deleted.
	 *
	 * @param dag_file The dag_file that is to be cleaned.
	 * @return MAKEFLOW_HOOK_SUCCESS is successful, MAKEFLOW_HOOK_FAILURE if not.
	 */
	int (*file_deleted)  (void * instance_struct, struct dag_file *file);
	
};

typedef enum {
    MAKEFLOW_HOOK_SUCCESS = 0,
    MAKEFLOW_HOOK_FAILURE,
    MAKEFLOW_HOOK_SKIP,
    MAKEFLOW_HOOK_END
} makeflow_hook_result;

struct batch_queue * makeflow_get_remote_queue();
struct batch_queue * makeflow_get_local_queue();
struct batch_queue * makeflow_get_queue(struct dag_node *node);

/** Add file to batch_task and DAG.
 *  This function takes a pattern for name_on_submission and name_on_exectution,
 *   replaces %% with the taskid, creates a dag_file to associate with the DAG, 
 *    and adds the specific names to the batch_task input files.
 *    @param d The DAG these files are created in.
 *    @param task The batch_task this file is added to.
 *    @param name_on_submission The pattern of the name from the submission/host site.
 *    @param name_on_execution The pattern of the name at execution site.
 *    @param file_type The type of file used in wrapper, GLOBAL or TEMP. TEMP is cleaned after each node.
 *    @return The DAG file that was either found or created in the dag.
 *    */
struct dag_file * makeflow_hook_add_input_file(struct dag *d, struct batch_task *task, const char * name_on_submission, const char * name_on_execution, dag_file_type_t file_type);

/** Add file to batch_task and DAG.
 *  This function takes a pattern for name_on_submission and name_on_exectution,
 *   replaces %% with the taskid, creates a dag_file to associate with the DAG, 
 *    and adds the specific names to the batch_task output files.
 *    @param d The DAG these files are created in.
 *    @param task The batch_task this file is added to.
 *    @param name_on_submission The pattern of the name from the submission/host site.
 *    @param name_on_execution The pattern of the name at execution site.
 *    @param file_type The type of file used in wrapper, GLOBAL or TEMP. TEMP is cleaned after each node.
 *    @return The DAG file that was either found or created in the dag.
 *    */
struct dag_file * makeflow_hook_add_output_file(struct dag *d, struct batch_task *task, const char * name_on_submission, const char * name_on_execution, dag_file_type_t file_type);

/** Add/Register makeflow_hook struct in list of hooks.
 Example of use see above.
@param hook The new hook to register.
*/
int makeflow_hook_register(struct makeflow_hook *hook, struct jx **args);

int makeflow_hook_create();

int makeflow_hook_destroy(struct dag *d);

int makeflow_hook_dag_check(struct dag *d);

int makeflow_hook_dag_clean(struct dag *d);

int makeflow_hook_dag_start(struct dag *d);

int makeflow_hook_dag_loop(struct dag *d);

int makeflow_hook_dag_end(struct dag *d);

int makeflow_hook_dag_fail(struct dag *d);

int makeflow_hook_dag_abort(struct dag *d);

int makeflow_hook_dag_success(struct dag *d);

int makeflow_hook_node_check(struct dag_node *node, struct batch_queue *queue);

int makeflow_hook_node_submit(struct dag_node *node, struct batch_task *task);

int makeflow_hook_batch_submit(struct batch_task *task);

int makeflow_hook_batch_retrieve(struct batch_task *task);

int makeflow_hook_node_end(struct dag_node *node, struct batch_task *task);

int makeflow_hook_node_success(struct dag_node *node, struct batch_task *task);

int makeflow_hook_node_fail(struct dag_node *node, struct batch_task *task);

int makeflow_hook_node_abort(struct dag_node *node);

int makeflow_hook_file_complete(struct dag_file *file);

int makeflow_hook_file_clean(struct dag_file *file);

int makeflow_hook_file_deleted(struct dag_file *file);
#endif
