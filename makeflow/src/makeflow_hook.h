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
#include "dag.h"
#include "dag_node.h"
#include "dag_file.h"
#include "hash_table.h"
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
 * register_hook(&makeflow_hook_example);
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
 * DAG: /TODO
 * Node: /TODO
 * File: /TODO
 */
struct makeflow_hook {

	const char * module_name;

	/* Initialize hooks.
	 *
	 * Initiallizing call to a hook. All arguments that are needed by the 
	 * hook should be added to a jx struct that is passed in to all create
	 * calls. This will allow for a variable number and set of arguments to
	 * a varying number of hooks.
	 *
	 * @param jx the struct with arguments for hook.
	 * @return 1 if successfully created, 0 if failed.
	 */
	int (*create)        (struct jx *hook_args);

	/* Destroy/Clean up hooks.
	 *
	 * Call when Makeflow is about to exit. This is to clean up any memory
	 * or structs left during execution. Most will be removed when exiting.
	 * This should only be used to clean up internal strucutures.
	 *
	 * @param d The DAG that is about to be destroyed.
	 * @return 1 if successfully destroyed, 0 if not.
	 */
	int (*destroy)       (struct dag *d);

	/* Hook prior to dag creation.
	 * 
	 * This is set after hook create, but prior to DAG creation.
	 * This is for adding features to the Makeflow environment, but not to
	 * the dag itself as it does not exist.
	 *
	 * @return 1 if dag init step successful, 0 if not.
	 */
	int (*dag_init)      ();

	/* Hook prior to dag start, but after parsing.
	 * 
	 * This is used to augment the DAG or utilize information from the
	 * DAG to make decisions. An example of this is the storage
	 * allocation module that looks at file information that is parsed.
	 *
	 * @param dag The DAG about to be started.
	 * @return 1 if dag start step successful, 0 if not.
	 */
	int (*dag_start)     (struct dag *d);

	/* Hook for a successfully completed DAG.
	 * 
	 * @param dag The DAG that was complete.
	 * @return 1 if dag end step successful, 0 if not.
	 */
	int (*dag_end)       (struct dag *d);

	/* Hook for a failed DAG.
	 * 
	 * This does not change that the DAG has failed, but gives the
	 * hook access to internal stats for failure analysis.
	 *
	 * @param dag The DAG that was failed.
	 * @return 1 if dag fail step successful, 0 if not.
	 */
	int (*dag_fail)      (struct dag *d);

	/* Hook for an aborted DAG.
	 * 
	 * This does not change that the DAG has aborted, but gives the
	 * hook access to internal stats for abort analysis.
	 *
	 * @param dag The DAG that was aborted.
	 * @return 1 if dag abort step successful, 0 if not.
	 */
	int (*dag_abort)     (struct dag *d);

	/* ADD WRAPPERS IN EITHER CREATE CHECK OR SUBMIT */

	/* Hook when a node is created.
	 * 
	 * This hook occurs during parse when a node is created. Is the first
	 * opportunity to see the command, files, env, and resources.
	 *
	 * @param dag_node The dag_node that was just created.
	 * @param batch_job_feature A strucuture that describes supported
	 *             features of used batch_job system.
	 * @return 1 if successful, 0 if not.
	 */
	int (*node_create)   (struct dag_node *n, struct hash_table *features);

	/* Hook when a node is checked for submission.
	 * 
	 * This hook occurs when nodes are checked for execution. This
	 * allows hooks to veto submission based on internal qualifiers.
	 * Example would be the storage allocation, or job limits.
	 *
	 * @param dag_node The dag_node that is being checked.
	 * @param batch_job_feature A strucuture that describes supported
	 *             features of used batch_job system.
	 * @return 1 if successful, 0 if not.
	 */
	int (*node_check)    (struct dag_node *n, struct hash_table *features);

	/* Hook just prior to node submission.
	 * 
	 * This hook occurs just before nodes are execution. This
	 * allows hooks augment submission within the Makeflow context.
	 *
	 * This is the correct location to `wrap` tasks using a wrapper for
	 * execution.
	 *
	 * @param dag_node The dag_node that is being checked.
	 * @return 1 if successful, 0 if not.
	 */
	int (*node_submit)   (struct dag_node *n);

	/* Augment/Modify the job strucuture passed to batch system.
	 *
	 * The intended use case of this is for performing batch specific
	 * modifications, such as sharedfs. Sharedfs is the example as 
	 * it does not change the actual structure of a job, but is the
	 * oppurtunity to change what is passed to the batch system. Files
	 * `forgotten` by sharedfs are not forgotten in Makeflow.
	 *
	 * The jx object that contains the job should be passed
	 * to allow for modifications of the structure.
	 *
	 * @param batch_queue specific queue being submitted to.
	 * @param jx the job specification as encoded like a wrapper (see json).
	 * @return 1 if successfully modified, 0 if not.
	 */
	int (*batch_submit) ( struct batch_queue *q, struct jx *task);


	/* Hook after node is collected, but prior to qualifying node success.
	 * 
	 * This hook occurs after node is retrieved from batch queue. This
	 * allows hooks to perform on outputted files and see exit status.
	 *
	 * @param dag_node The dag_node that was collected.
	 * @param batch_job_info The info struct passed by batch queue.
	 * @return 1 if successful, 0 if not.
	 */
	int (*node_end)      (struct dag_node *n, struct batch_job_info *info);

	/* Hook if node was successful.
	 * 
	 * @param dag_node The dag_node that was successful.
	 * @param batch_job_info The info struct passed by batch queue.
	 * @return 1 if successful, 0 if not.
	 */
	int (*node_success)  (struct dag_node *n, struct batch_job_info *info);

	/* Hook if node failed.
	 * 
	 * @param dag_node The dag_node that failed.
	 * @param batch_job_info The info struct passed by batch queue.
	 * @return 1 if successful, 0 if not.
	 */
	int (*node_fail)     (struct dag_node *n, struct batch_job_info *info);

	/* Hook if node aborted.
	 * 
	 * @param dag_node The dag_node that was aborted.
	 * @param batch_job_info The info struct passed by batch queue.
	 * @return 1 if successful, 0 if not.
	 */
	int (*node_abort)    (struct dag_node *n, struct batch_job_info *info);

	/* Hook when file is created.
	 * 
	 * Allows modifications when file is created.
	 *
	 * @param dag_file The dag_file that was initialized.
	 * @return 1 is successful, 0 if not.
	 */
	int (*file_create)   (struct dag_file *f);

	/* Hook when file is expected, prior to node submission.
	 *
	 * @param dag_file The dag_file that is expected.
	 * @return 1 is successful, 0 if not.
	 */
	int (*file_expect)   (struct dag_file *f);

	/* Hook when file is registered as existing.
	 *
	 * @param dag_file The dag_file that exists.
	 * @return 1 is successful, 0 if not.
	 */
	int (*file_exist)    (struct dag_file *f);

	/* Hook when file is registered as complete.
	 *
	 * Complete means that the file still exists, 
	 * but is no longer used. The next step is to 
	 * clean it.
	 *
	 * @param dag_file The dag_file that completed.
	 * @return 1 is successful, 0 if not.
	 */
	int (*file_complete) (struct dag_file *f);

	/* Hook when file is about to be clean.
	 *
	 * This is used for archiving files are storing files prior
	 * to removal.
	 *
	 * @param dag_file The dag_file that is to be cleaned.
	 * @return 1 is successful, 0 if not.
	 */
	int (*file_clean)    (struct dag_file *f);

	/* Hook when file has been deleted.
	 *
	 * @param dag_file The dag_file that is to be cleaned.
	 * @return 1 is successful, 0 if not.
	 */
	int (*file_deleted)  (struct dag_file *f);
	
};

typedef enum {
    MAKEFLOW_HOOK_SUCCESS = 0,
    MAKEFLOW_HOOK_FAILURE
} makeflow_hook_return;

struct batch_queue * makeflow_get_remote_queue();
struct batch_queue * makeflow_get_local_queue();

void makeflow_hook_register(struct makeflow_hook *hook);

int makeflow_hook_create(struct jx *args);

int makeflow_hook_destroy(struct dag *d);

int makeflow_hook_dag_init(struct dag *d);

int makeflow_hook_dag_parse(struct dag *d);

int makeflow_hook_dag_start(struct dag *d);

int makeflow_hook_dag_end(struct dag *d);

int makeflow_hook_dag_fail(struct dag *d);

int makeflow_hook_dag_abort(struct dag *d);

int makeflow_hook_node_create(struct dag_node *n, struct hash_table *feat);

int makeflow_hook_node_submit(struct dag_node *n);

int makeflow_hook_node_success(struct dag_node *n, struct batch_job_info *bji);

int makeflow_hook_node_fail(struct dag_node *n, struct batch_job_info *bji);

int makeflow_hook_node_abort(struct dag_node *n, struct batch_job_info *bji);

#endif
