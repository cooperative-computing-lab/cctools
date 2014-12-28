/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "dag_log.h"
#include "dag.h"
#include "get_line.h"

#include "timestamp.h"
#include "list.h"

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

void dag_log_recover(struct dag *d, const char *filename, int verbose_mode )
{
	char *line;
	int nodeid, state, jobid;
	int first_run = 1;
	struct dag_node *n;
	timestamp_t previous_completion_time;

	d->logfile = fopen(filename, "r");
	if(d->logfile) {
		int linenum = 0;
		first_run = 0;

		printf("recovering from log file %s...\n",filename);

		while((line = get_line(d->logfile))) {
			linenum++;

			if(line[0] == '#')
				continue;
			if(sscanf(line, "%" SCNu64 " %d %d %d", &previous_completion_time, &nodeid, &state, &jobid) == 4) {
				n = itable_lookup(d->node_table, nodeid);
				if(n) {
					n->state = state;
					n->jobid = jobid;
					/* Log timestamp is in microseconds, we need seconds for diff. */
					n->previous_completion = (time_t) (previous_completion_time / 1000000);
					continue;
				}
			}

			fprintf(stderr, "makeflow: %s appears to be corrupted on line %d\n", filename, linenum);
			exit(1);
		}
		fclose(d->logfile);
	}

	d->logfile = fopen(filename, "a");
	if(!d->logfile) {
		fprintf(stderr, "makeflow: couldn't open logfile %s: %s\n", filename, strerror(errno));
		exit(1);
	}
	if(setvbuf(d->logfile, NULL, _IOLBF, BUFSIZ) != 0) {
		fprintf(stderr, "makeflow: couldn't set line buffer on logfile %s: %s\n", filename, strerror(errno));
		exit(1);
	}

	if(first_run && verbose_mode) {
		struct dag_file *f;
		struct dag_node *p;
		for(n = d->nodes; n; n = n->next) {
			/* Record node information to log */
			fprintf(d->logfile, "# NODE\t%d\t%s\n", n->nodeid, n->command);

			/* Record the node category to the log */
			fprintf(d->logfile, "# SYMBOL\t%d\t%s\n", n->nodeid, n->category->label);

			/* Record node parents to log */
			fprintf(d->logfile, "# PARENTS\t%d", n->nodeid);
			list_first_item(n->source_files);
			while( (f = list_next_item(n->source_files)) ) {
				p = f->target_of;
				if(p)
					fprintf(d->logfile, "\t%d", p->nodeid);
			}
			fputc('\n', d->logfile);

			/* Record node inputs to log */
			fprintf(d->logfile, "# SOURCES\t%d", n->nodeid);
			list_first_item(n->source_files);
			while( (f = list_next_item(n->source_files)) ) {
				fprintf(d->logfile, "\t%s", f->filename);
			}
			fputc('\n', d->logfile);

			/* Record node outputs to log */
			fprintf(d->logfile, "# TARGETS\t%d", n->nodeid);
			list_first_item(n->target_files);
			while( (f = list_next_item(n->target_files)) ) {
				fprintf(d->logfile, "\t%s", f->filename);
			}
			fputc('\n', d->logfile);

			/* Record translated command to log */
			fprintf(d->logfile, "# COMMAND\t%d\t%s\n", n->nodeid, n->command);
		}
	}


	dag_count_states(d);

	// Decide rerun tasks
	if(!first_run) {
		struct itable *rerun_table = itable_create(0);
		for(n = d->nodes; n; n = n->next) {
			dag_node_decide_rerun(rerun_table, d, n);
		}
		itable_delete(rerun_table);
	}

	//Update file reference counts from nodes in log
	for(n = d->nodes; n; n = n->next) {
		if(n->state == DAG_NODE_STATE_COMPLETE)
		{
			struct dag_file *f;
			list_first_item(n->source_files);
			while((f = list_next_item(n->source_files)))
				f->ref_count += -1;
		}
	}
}

