/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BATCH_TASK_H
#define BATCH_TASK_H

#include "batch_file.h"
#include "list.h"
#include "jx.h"
#include "rmsummary.h"

struct batch_task {
	int taskid;
	int jobid;
	struct batch_queue *queue; /* queue this file is being created for. */

	char *command;         /* The command line to execute. */

	struct list   *input_files;  /* Task's required inputs */
	struct list   *output_files; /* Task's expected outputs */

	struct rmsummary *resources; /* Resources resolved from category and node */

	struct jx *envlist;      /* JSON formatted environment list */ 
};

struct batch_task *batch_task_create(struct batch_queue *queue );

void batch_task_delete(struct batch_task *t);

struct batch_file * batch_task_add_input_file(struct batch_task *task, char * filename, char * remote_name);
struct batch_file * batch_task_add_output_file(struct batch_task *task, char * filename, char * remote_name);

void batch_task_wrap_command(struct batch_task *t, char *command);

#endif
/* vim: set noexpandtab tabstop=4: */
