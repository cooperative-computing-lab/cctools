#include "vine_file.h"
#include "vine_manager.h"
#include "vine_mount.h"
#include "vine_task.h"

#include "path.h"

#include <inttypes.h>
#include <stdio.h>

static int show_names = 0;

void vine_taskgraph_log_write_header(struct vine_manager *q)
{
	fprintf(q->graph_logfile, "digraph \"taskvine\" {\n");
	fprintf(q->graph_logfile, "node [style=filled,font=Helvetica,fontsize=10];\n");
}

void vine_taskgraph_log_write_task(struct vine_manager *q, struct vine_task *t)
{
	if (!t)
		return;

	int id = t->task_id;

	char *name = strdup(t->command_line);
	char *p = strchr(name, ' ');
	if (p)
		*p = 0;

	fprintf(q->graph_logfile,
			"\"task-%d\" [color=green,label=\"%s\"];\n",
			id,
			show_names ? path_basename(name) : "");

	free(name);

	struct vine_mount *m;

	LIST_ITERATE(t->input_mounts, m)
	{
		fprintf(q->graph_logfile, "\"file-%s\" -> \"task-%d\";\n", m->file->cached_name, id);
	}

	LIST_ITERATE(t->output_mounts, m)
	{
		fprintf(q->graph_logfile, "\"task-%d\" -> \"file-%s\";\n", id, m->file->cached_name);
	}
}

void vine_taskgraph_log_write_mini_task(
		struct vine_manager *q, struct vine_task *t, const char *task_name, const char *output_name)
{
	if (!t)
		return;

	/* XXX Mini-tasks do not have unique ID numbers, so make it up from the pointer address. */
	int id = (intptr_t)t;

	char *name = strdup(t->command_line);
	char *p = strchr(name, ' ');
	if (p)
		*p = 0;

	fprintf(q->graph_logfile, "\"task-%d\" [color=green,label=\"%s\"];\n", id, show_names ? task_name : "");

	free(name);

	struct vine_mount *m;

	LIST_ITERATE(t->input_mounts, m)
	{
		fprintf(q->graph_logfile, "\"file-%s\" -> \"task-%d\";\n", m->file->cached_name, id);
	}

	/* A mini-task has one implied output that is named by provided argument, not the data structure */
	fprintf(q->graph_logfile, "\"task-%d\" -> \"file-%s\";\n", id, output_name);
}

void vine_taskgraph_log_write_file(struct vine_manager *q, struct vine_file *f)
{
	if (!f)
		return;

	fprintf(q->graph_logfile,
			"\"file-%s\" [shape=rect,color=blue,label=\"%s\"];\n",
			f->cached_name,
			(show_names && f->source) ? path_basename(f->source) : "");
	vine_taskgraph_log_write_mini_task(q, f->mini_task, f->source, f->cached_name);
}

void vine_taskgraph_log_write_footer(struct vine_manager *q) { fprintf(q->graph_logfile, "}\n"); }
