#include "vine_manager.h"
#include "vine_task.h"
#include "vine_file.h"
#include "vine_mount.h"

#include "path.h"

#include <stdio.h>
#include <inttypes.h>

void vine_graph_log_write_header( struct vine_manager *q )
{
	fprintf(q->graph_logfile,"digraph \"taskvine\" {\n");
	fprintf(q->graph_logfile,"node [style=filled,font=Helvetica,fontsize=10];\n");
}

void vine_graph_log_write_task( struct vine_manager *q, struct vine_task *t )
{
	fprintf(q->graph_logfile,"\"task-%d\" [color=green,label=\"%d\"];\n",t->task_id,t->task_id);

	struct vine_mount *m;

	LIST_ITERATE(t->input_mounts,m) {
		fprintf(q->graph_logfile,"\"file-%s\" -> \"task-%d\";\n",m->file->cached_name,t->task_id);
	}	

	LIST_ITERATE(t->output_mounts,m) {
		fprintf(q->graph_logfile,"\"task-%d\" -> \"file-%s\";\n",t->task_id,m->file->cached_name);
	}	
}

void vine_graph_log_write_file( struct vine_manager *q, struct vine_file *f )
{
	fprintf(q->graph_logfile,"\"file-%s\" [shape=rect,color=blue,label=\"%s\"];\n",f->cached_name,f->source ? path_basename(f->source) : "");
}

void vine_graph_log_write_footer( struct vine_manager *q )
{
	fprintf(q->graph_logfile,"}\n");
}

