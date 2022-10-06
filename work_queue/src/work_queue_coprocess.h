/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/* return the name of the coprocess */
char *work_queue_coprocess_start(char *coprocess_command, int *coprocess_port);
void work_queue_coprocess_terminate();
int work_queue_coprocess_check();
char *work_queue_coprocess_run(const char *function_name, const char *function_input, int coprocess_port);

