/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "dag.h"
#include "timestamp.h"
#include "stringtools.h"
#include "list.h"
#include "makeflow_summary.h"
#include "catalog_query.h"
#include "username.h"
#include "batch_job.h"
#include "jx_print.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>

//packet for start and stop, once per min, configurable time

//make sure to send: project name, owner, type

//waiting means: inputs not ready, running means: took job and submitted

int	makeflow_catalog_summary (struct dag *d, char *name, batch_queue_type_t type, timestamp_t start){
  	struct dag_node *n;
  	dag_node_state_t state;

  	int tasks_completed = 0;
  	int tasks_aborted = 0;
  	int tasks_waiting = 0;
  	int tasks_running = 0;
  	int tasks_failed = 0;

  	for (n = d->nodes; n; n = n->next){
      	state = n->state;
      	if (state == DAG_NODE_STATE_FAILED)
			tasks_failed++;
      	else if (state == DAG_NODE_STATE_ABORTED)
			tasks_aborted++;
      	else if (state == DAG_NODE_STATE_COMPLETE)
			tasks_completed++;
      	else if (state == DAG_NODE_STATE_RUNNING)
			tasks_running++;
      	else if (state == DAG_NODE_STATE_WAITING)
			tasks_waiting++;
    }

 	 //transmit report here
  	char *host = CATALOG_HOST;

  	char username[USERNAME_MAX];
  	username_get (username);

  	const char *batch_type = batch_queue_type_to_string (type);

  	struct jx *j = jx_object (0);

  	jx_insert_string (j, "type", "makeflow");
  	jx_insert_integer (j, "total", itable_size (d->node_table));
  	jx_insert_integer (j, "running", tasks_running);
  	jx_insert_integer (j, "waiting", tasks_waiting);
  	jx_insert_integer (j, "aborted", tasks_aborted);
  	jx_insert_integer (j, "completed", tasks_completed);
  	jx_insert_integer (j, "failed", tasks_failed);
  	jx_insert_string (j, "project", name);
  	jx_insert_string (j, "owner", username);
  	char *timestring = string_format ("%" PRIu64 "", start);
  	jx_insert_string (j, "time_started", timestring);
  	jx_insert_string (j, "batch_type", batch_type);



  	//creates memory
  	char *text = jx_print_string (j);

  	int resp = catalog_query_send_update(host,text,CATALOG_UPDATE_BACKGROUND);

  	free (text);
  	free (timestring);
  	jx_delete (j);

  	return resp;			//all good
}

//print to file the status of makeflow, default to once per min, configurable time

// make sure to send: project name, owner, type

// waiting means: input not ready, running means: took job and submitted

int makeflow_file_summary (struct dag *d, char *name, batch_queue_type_t type, timestamp_t start, char *status_file_name){

	struct dag_node *n;
  	dag_node_state_t state;

  	int tasks_completed = 0;
  	int tasks_aborted = 0;
  	int tasks_waiting = 0;
  	int tasks_running = 0;
  	int tasks_failed = 0;

  	for (n = d->nodes; n; n = n->next){
      	state = n->state;
      	if (state == DAG_NODE_STATE_FAILED)
			tasks_failed++;
      	else if (state == DAG_NODE_STATE_ABORTED)
			tasks_aborted++;
      	else if (state == DAG_NODE_STATE_COMPLETE)
			tasks_completed++;
      	else if (state == DAG_NODE_STATE_RUNNING)
			tasks_running++;
      	else if (state == DAG_NODE_STATE_WAITING)
			tasks_waiting++;
    }

 	 char username[USERNAME_MAX];
  	username_get (username);

  	const char *batch_type = batch_queue_type_to_string (type);

  	FILE *fp;
  	fp = fopen (status_file_name, "w");
  	fprintf (fp, "<!DOCTYPE html>\n");
  	fprintf (fp, "<html>\n");
  	fprintf (fp, "<body>\n");
  	fprintf (fp, "<h1>Jobs Status</h1>\n");
  	int total_tasks = itable_size (d->node_table);
  	fprintf (fp, "<p>Total: %d </p>\n", total_tasks);
  	fprintf (fp, "<p>Running: %d </p>\n", tasks_running);
  	fprintf (fp, "<p>Waiting: %d </p>\n", tasks_waiting);
  	fprintf (fp, "<p>Aborted: %d </p>\n", tasks_aborted);
  	fprintf (fp, "<p>Completed: %d </p>\n", tasks_completed);
  	fprintf (fp, "<p>Failed: %d </p>\n", tasks_failed);
  	fprintf (fp, "<p>Project: %s </p>\n", name);
  	fprintf (fp, "<p>Owner: %s </p>\n", username);

	timestamp_t now = timestamp_get ();
  	timestamp_t time_from_start = ((now - start) / 1000) / 1000;
  	char *timestring = string_format ("%" PRIu64 "", time_from_start);
  	fprintf (fp, "<p>Time from started: %s seconds</p>\n", timestring);
  	fprintf (fp, "<p>Batch type: %s </p>\n", batch_type);
  	float percent_task_waiting = tasks_waiting / (float) total_tasks * 100;
  	float percent_task_running = tasks_running / (float) total_tasks * 100;
  	float percent_task_completed = tasks_completed / (float) total_tasks * 100;
  	float percent_other = 100 - percent_task_waiting - percent_task_running -
	    percent_task_completed;
  	fprintf (fp, "<p> Waiting (W): yellow, %.2f", percent_task_waiting);
	fprintf (fp, "%%; Running (R): blue, %.2f", percent_task_running);
	fprintf (fp, "%%</p>\n");
  	fprintf (fp, "<p> Completed (C): green, %.2f", percent_task_completed);
	fprintf (fp, "%%; Other (O): red, %.2f", percent_other);
	fprintf (fp, "%%</p>\n");
  	fprintf (fp, "<table style='height:50px;width:100%%'>\n");
  	fprintf (fp, "<tr>\n");

  	fprintf (fp, "<td style='background-color:#dfff33; width: %.2f", percent_task_waiting);
  	fprintf (fp, "%%'></td>\n");

  	fprintf (fp, "<td style='background-color:#3369ff; width: %.2f", percent_task_running);
  	fprintf (fp, "%%'></td>\n");

  	fprintf (fp, "<td style='background-color:#47ff33; width: %.2f", percent_task_completed);
   	fprintf (fp, "%%'></td>\n");

  	fprintf (fp, "<td style='background-color:#ff3355; width: %.2f", percent_other);
  	fprintf (fp, "%%'></td>\n");
  	fprintf (fp, "</tr>\n");
  	fprintf (fp, "</table>\n");
  	fprintf (fp, "</body>\n");
  	fprintf (fp, "</html>\n");

  	fclose (fp);
  	free (timestring);
  return 1;	//all good
}
