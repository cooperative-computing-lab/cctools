/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

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

int vine_coprocess_start(struct vine_coprocess *coprocess);
void vine_coprocess_terminate(struct vine_coprocess *coprocess);
void vine_coprocess_shutdown(struct vine_coprocess *coprocess_info, int num_coprocesses);
int vine_coprocess_check(struct vine_coprocess *coprocess);
char *vine_coprocess_run(const char *function_name, const char *function_input, struct vine_coprocess *coprocess);
struct vine_coprocess *vine_coprocess_find_state(struct vine_coprocess *coprocess_info, int number_of_coprocesses, vine_coprocess_state_t state);
struct vine_coprocess *vine_coprocess_initalize_all_coprocesses(int coprocess_cores, int coprocess_memory, int coprocess_disk, int coprocess_gpus, struct vine_resources *total_resources, char *coprocess_command, int number_of_coprocess_instances); 
void vine_coprocess_shutdown_all_coprocesses(struct vine_coprocess *coprocess_info, int number_of_coprocesses);
void vine_coprocess_measure_resources(struct vine_coprocess *coprocess_info, int number_of_coprocesses);
int vine_coprocess_enforce_limit(struct vine_coprocess *coprocess);
void vine_coprocess_update_state(struct vine_coprocess *coprocess_info, int number_of_coprocesses);
