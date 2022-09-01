/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef WORK_QUEUE_COPROCESS_H
#define WORK_QUEUE_COPROCESS_H
#include "link.h"
#include "work_queue_resources.h"

typedef enum {
	WORK_QUEUE_COPROCESS_UNINITIALIZED, /**< worker has not yet created coprocess instance **/
	WORK_QUEUE_COPROCESS_READY,         /**< coprocess is ready to receive and run a RemoteTask **/
	WORK_QUEUE_COPROCESS_RUNNING,       /**< coprocess is currently running a RemoteTask and is busy **/
	WORK_QUEUE_COPROCESS_DEAD           /**< coprocess has died and needs to be restarted **/
} work_queue_coprocess_state_t;

struct work_queue_coprocess {
    char *command;
    char *name;
    int port;
    int pid;
    work_queue_coprocess_state_t state;
    int pipe_in[2];
    int pipe_out[2];
    struct link *link;
    int num_restart_attempts;
    struct work_queue_resources *coprocess_resources;
};

/* return the name of the coprocess */
char *work_queue_coprocess_start(struct work_queue_coprocess *coprocess);
void work_queue_coprocess_terminate(struct work_queue_coprocess *coprocess);
void work_queue_coprocess_shutdown(struct work_queue_coprocess *coprocess_info, int num_coprocesses);
int work_queue_coprocess_check(struct work_queue_coprocess *coprocess);
char *work_queue_coprocess_run(const char *function_name, const char *function_input, struct work_queue_coprocess *coprocess);
struct work_queue_coprocess *work_queue_coprocess_find_state(struct work_queue_coprocess *coprocess_info, int number_of_coprocesses, work_queue_coprocess_state_t state);
void work_queue_coprocess_measure_resources(struct work_queue_coprocess *coprocess_info, int number_of_coprocesses);
int work_queue_coprocess_enforce_limit(struct work_queue_coprocess *coprocess);
void work_queue_coprocess_update_state(struct work_queue_coprocess *coprocess_info, int number_of_coprocesses);

#endif