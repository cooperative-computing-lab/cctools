#include "vine_manager.h"
#include "vine_task.h"
#include "vine_file.h"

#include <stdio.h>
#include <inttypes.h>

void vine_graph_log_write_header( struct vine_manager *q )
{
	fprintf(q->graph_logfile,"digraph \"taskvine\" {\n");
}

void vine_graph_log_write_task( struct vine_manager *q, struct vine_task *t )
{
	fprintf(q->graph_logfile,"task-%"PRId64" [];\n",t->task_id);
}

void vine_graph_log_write_file( struct vine_manager *q, struct vine_file *f )
{
	fprintf(q->graph_logfile,"file-%s [];\n",f->cached_name);
}

void vine_graph_log_write_footer( struct vine_manager *q )
{
	fprintf(q->graph_logfile,"}\n");
}

