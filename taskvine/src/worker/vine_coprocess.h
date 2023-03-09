/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#include "list.h"
#include "taskvine.h"
typedef enum {
	VINE_COPROCESS_UNINITIALIZED, /**< worker has not yet created coprocess instance **/
	VINE_COPROCESS_READY,         /**< coprocess is ready to receive and run a RemoteTask **/
	VINE_COPROCESS_RUNNING,       /**< coprocess is currently running a RemoteTask and is busy **/
	VINE_COPROCESS_DEAD           /**< coprocess has died and needs to be restarted **/
} vine_coprocess_state_t;

struct vine_coprocess {
    char *command;
    char *name;
    int port;
    int pid;
    vine_coprocess_state_t state;
    int pipe_in[2];
    int pipe_out[2];
    struct link *read_link;
    struct link *write_link;
    struct link *network_link;
    int num_restart_attempts;
    struct vine_resources *coprocess_resources;
};

int vine_coprocess_start(struct vine_coprocess *coprocess, char *sandbox);
void vine_coprocess_terminate(struct vine_coprocess *coprocess);
int vine_coprocess_check(struct vine_coprocess *coprocess);
char *vine_coprocess_run(const char *function_name, const char *function_input, struct vine_coprocess *coprocess);
struct vine_coprocess *vine_coprocess_find_state(struct list *coprocess_list, vine_coprocess_state_t state, char *coprocess_name);
struct vine_coprocess *vine_coprocess_initialize_coprocess(char *coprocess_command);
void vine_coprocess_specify_resources(struct vine_coprocess *coprocess, struct rmsummary *allocated_resources);
void vine_coprocess_shutdown_all_coprocesses(struct list *coprocess_list);
void vine_coprocess_measure_resources(struct list *coprocess_list);
int vine_coprocess_enforce_limit(struct vine_coprocess *coprocess);
void vine_coprocess_update_state(struct list *coprocess_list);
