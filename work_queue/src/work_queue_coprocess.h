/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef WORK_QUEUE_COPROCESS_H
#define WORK_QUEUE_COPROCESS_H
#include "link.h"

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
    struct link *link;
};

extern struct work_queue_coprocess *coprocess_info;

/* return the name of the coprocess */
char *work_queue_coprocess_start(char *coprocess_command, struct work_queue_coprocess *coprocess);
void work_queue_coprocess_terminate();
int work_queue_coprocess_check();
char *work_queue_coprocess_run(const char *function_name, const char *function_input, struct work_queue_coprocess *coprocess);
int work_queue_coprocess_find_state(struct work_queue_coprocess *coprocess_info, int number_of_coprocesses, work_queue_coprocess_state_t state);

#endif