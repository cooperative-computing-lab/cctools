/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "dag.h"
#include "timestamp.h"
#include "stringtools.h"
#include "list.h"

#include <stdio.h>
#include <stdarg.h>
#include <string.h>

static void summarize(FILE * file, FILE * email, const char *format, ...)
{
	va_list args;
	if(file) {
		va_start(args, format);
		vfprintf(file, format, args);
		va_end(args);
	}
	if(email) {
		va_start(args, format);
		vfprintf(email, format, args);
		va_end(args);
	}
}

void makeflow_summary_create(struct dag *d, const char *filename, const char *email_summary_to, timestamp_t runtime, timestamp_t time_completed, int argc, char *argv[], const char *dagfile, struct batch_queue *remote_queue, int abort_flag, int failed_flag )
{
	char buffer[50];

	FILE *summary_file = NULL;
	FILE *summary_email = NULL;

	if(filename)
		summary_file = fopen(filename, "w");

	if(email_summary_to) {
		summary_email = popen("sendmail -t", "w");
		fprintf(summary_email, "To: %s\n", email_summary_to);
		timestamp_fmt(buffer, 50, "%c", time_completed);
		fprintf(summary_email, "Subject: Makeflow Run Summary - %s \n", buffer);
	}

	int i;

	for(i = 0; i < argc; i++)
		summarize(summary_file, summary_email, "%s ", argv[i]);

	summarize(summary_file, summary_email, "\n");

	if(abort_flag)
		summarize(summary_file, summary_email, "Workflow aborted:\t ");
	else if(failed_flag)
		summarize(summary_file, summary_email, "Workflow failed:\t ");
	else
		summarize(summary_file, summary_email, "Workflow completed:\t ");
	timestamp_fmt(buffer, 50, "%c\n", time_completed);
	summarize(summary_file, summary_email, "%s", buffer);

	int seconds = runtime / 1000000;
	int hours = seconds / 3600;
	int minutes = (seconds - hours * 3600) / 60;
	seconds = seconds - hours * 3600 - minutes * 60;
	summarize(summary_file, summary_email, "Total runtime:\t\t %d:%02d:%02d\n", hours, minutes, seconds);

	summarize(summary_file, summary_email, "Workflow file:\t\t %s\n", dagfile);

	struct dag_node *n;
	struct dag_file *f;
	const char *fn = NULL;
	dag_node_state_t state;
	struct list *output_files;
	output_files = list_create();
	struct list *failed_tasks;
	failed_tasks = list_create();
	int total_tasks = itable_size(d->node_table);
	int tasks_completed = 0;
	int tasks_aborted = 0;
	int tasks_unrun = 0;

	for(n = d->nodes; n; n = n->next) {
		state = n->state;
		if(state == DAG_NODE_STATE_FAILED && !list_find(failed_tasks, (int (*)(void *, const void *)) string_equal, (void *) fn))
			list_push_tail(failed_tasks, (void *) n->command);
		else if(state == DAG_NODE_STATE_ABORTED)
			tasks_aborted++;
		else if(state == DAG_NODE_STATE_COMPLETE) {
			tasks_completed++;
			list_first_item(n->source_files);
			while((f = list_next_item(n->source_files))) {
				fn = f->filename;
				if(!list_find(output_files, (int (*)(void *, const void *)) string_equal, (void *) fn))
					list_push_tail(output_files, (void *) fn);
			}
		} else
			tasks_unrun++;
	}

	summarize(summary_file, summary_email, "Number of tasks:\t %d\n", total_tasks);
	summarize(summary_file, summary_email, "Completed tasks:\t %d/%d\n", tasks_completed, total_tasks);
	if(tasks_aborted != 0)
		summarize(summary_file, summary_email, "Aborted tasks:\t %d/%d\n", tasks_aborted, total_tasks);
	if(tasks_unrun != 0)
		summarize(summary_file, summary_email, "Tasks not run:\t\t %d/%d\n", tasks_unrun, total_tasks);
	if(list_size(failed_tasks) > 0)
		summarize(summary_file, summary_email, "Failed tasks:\t\t %d/%d\n", list_size(failed_tasks), total_tasks);
	for(list_first_item(failed_tasks); (fn = list_next_item(failed_tasks)) != NULL;)
		summarize(summary_file, summary_email, "\t%s\n", fn);

	if(list_size(output_files) > 0) {
		summarize(summary_file, summary_email, "Output files:\n");
		for(list_first_item(output_files); (fn = list_next_item(output_files)) != NULL;) {
			const char *size;
			struct stat buf;
			stat(fn, &buf);
			size = string_metric(buf.st_size, -1, NULL);
			summarize(summary_file, summary_email, "\t%s\t%s\n", fn, size);
		}
	}

	list_free(output_files);
	list_delete(output_files);
	list_free(failed_tasks);
	list_delete(failed_tasks);

	if(filename) {
		fprintf(stderr, "writing summary to %s.\n", filename);
		fclose(summary_file);
	}

	if(email_summary_to) {
		fprintf(stderr, "emailing summary to %s.\n", email_summary_to);
		fclose(summary_email);
	}
}

/* vim: set noexpandtab tabstop=4: */
