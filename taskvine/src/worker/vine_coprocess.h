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

struct vine_coprocess;

vine_coprocess_state_t vine_coprocess_state( struct vine_coprocess *c );
void vine_coprocess_state_set( struct vine_coprocess *c, vine_coprocess_state_t state );
const char * vine_coprocess_name( struct vine_coprocess *c );

int vine_coprocess_start(struct vine_coprocess *coprocess, char *sandbox);
void vine_coprocess_terminate(struct vine_coprocess *coprocess);
int vine_coprocess_check(struct vine_coprocess *coprocess);
char *vine_coprocess_run(const char *function_name, const char *function_input, const char *sandbox, struct vine_coprocess *coprocess);
struct vine_coprocess *vine_coprocess_find_state(struct list *coprocess_list, vine_coprocess_state_t state, char *coprocess_name);
struct vine_coprocess *vine_coprocess_initialize_coprocess(char *coprocess_command);
void vine_coprocess_specify_resources(struct vine_coprocess *coprocess);
void vine_coprocess_shutdown_all_coprocesses(struct list *coprocess_list);
void vine_coprocess_measure_resources(struct list *coprocess_list);
int vine_coprocess_enforce_limit(struct vine_coprocess *coprocess);

