/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_txn_log.h"
#include "vine_worker_info.h"
#include "vine_task.h"
#include "vine_file.h"

#include "buffer.h"
#include "rmsummary.h"
#include "macros.h"
#include "jx.h"
#include "jx_print.h"
#include "rmsummary.h"
#include "rmonitor_types.h"

#include <stdio.h>
#include <unistd.h>

void vine_txn_log_write(struct vine_manager *q, const char *str)
{
	if(!q->txn_logfile)
		return;

	fprintf(q->txn_logfile, "%" PRIu64 " %d %s\n", timestamp_get(),getpid(),str);
	fflush(q->txn_logfile);
}



void vine_txn_log_write_header( struct vine_manager *q )
{
	setvbuf(q->txn_logfile, NULL, _IOLBF, 1024); // line buffered, we don't want incomplete lines

	fprintf(q->txn_logfile, "# time manager_pid MANAGER manager_pid START|END\n");
	fprintf(q->txn_logfile, "# time manager_pid WORKER worker_id CONNECTION host:port\n");
	fprintf(q->txn_logfile, "# time manager_pid WORKER worker_id DISCONNECTION (UNKNOWN|IDLE_OUT|FAST_ABORT|FAILURE|STATUS_WORKER|EXPLICIT)\n");
	fprintf(q->txn_logfile, "# time manager_pid WORKER worker_id RESOURCES {resources}\n");
	fprintf(q->txn_logfile, "# time manager_pid WORKER worker_id CACHE_UPDATE filename sizeinmb walltime\n");
	fprintf(q->txn_logfile, "# time manager_pid WORKER worker_id TRANSFER (INPUT|OUTPUT) filename sizeinmb walltime\n");
	fprintf(q->txn_logfile, "# time manager_pid CATEGORY name MAX {resources_max_per_task}\n");
	fprintf(q->txn_logfile, "# time manager_pid CATEGORY name MIN {resources_min_per_task_per_worker}\n");
	fprintf(q->txn_logfile, "# time manager_pid CATEGORY name FIRST (FIXED|MAX|MIN_WASTE|MAX_THROUGHPUT) {resources_requested}\n");
	fprintf(q->txn_logfile, "# time manager_pid TASK task_id WAITING category_name (FIRST_RESOURCES|MAX_RESOURCES) {resources_requested}\n");
	fprintf(q->txn_logfile, "# time manager_pid TASK task_id RUNNING worker_id (FIRST_RESOURCES|MAX_RESOURCES) {resources_allocated}\n");
	fprintf(q->txn_logfile, "# time manager_pid TASK task_id WAITING_RETRIEVAL worker_id\n");
	fprintf(q->txn_logfile, "# time manager_pid TASK task_id (RETRIEVED|DONE) (SUCCESS|SIGNAL|END_TIME|FORSAKEN|MAX_RETRIES|MAX_WALLTIME|UNKNOWN|RESOURCE_EXHAUSTION) exit_code {limits_exceeded} {resources_measured}\n");
	fprintf(q->txn_logfile, "\n");
}

void vine_txn_log_write_task(struct vine_manager *q, struct vine_task *t)
{
	if(!q->txn_logfile)
		return;

	struct buffer B;
	buffer_init(&B);

	vine_task_state_t state = t->state;

	buffer_printf(&B, "TASK %d %s", t->task_id, vine_task_state_to_string(state));

	if(state == VINE_TASK_UNKNOWN) {
			/* do not add any info */
	} else if(state == VINE_TASK_READY) {
		const char *allocation = (t->resource_request == CATEGORY_ALLOCATION_FIRST ? "FIRST_RESOURCES" : "MAX_RESOURCES");
		buffer_printf(&B, " %s %s ", t->category, allocation);
		rmsummary_print_buffer(&B, vine_manager_task_resources_min(q, t), 1);
	} else if(state == VINE_TASK_CANCELED) {
			/* do not add any info */
	} else if(state == VINE_TASK_RETRIEVED || state == VINE_TASK_DONE) {
		buffer_printf(&B, " %s ", vine_result_string(t->result));
		buffer_printf(&B, " %d ", t->exit_code);

		if(t->resources_measured) {
			if(t->result == VINE_RESULT_RESOURCE_EXHAUSTION) {
				rmsummary_print_buffer(&B, t->resources_measured->limits_exceeded, 1);
				buffer_printf(&B, " ");
			}
			else {
				// no limits broken, thus printing an empty dictionary
				buffer_printf(&B, " {} ");
			}

			struct jx *m = rmsummary_to_json(t->resources_measured, /* only resources */ 1);
			jx_insert(m, jx_string("vine_input_size"), jx_arrayv(jx_double(t->bytes_sent/((double) MEGABYTE)), jx_string("MB"), NULL));
			jx_insert(m, jx_string("vine_output_size"), jx_arrayv(jx_double(t->bytes_received/((double) MEGABYTE)), jx_string("MB"), NULL));
			jx_insert(m, jx_string("vine_input_time"), jx_arrayv(jx_double((t->time_when_commit_end - t->time_when_commit_start)/((double) ONE_SECOND)), jx_string("s"), NULL));
			jx_insert(m, jx_string("vine_output_time"), jx_arrayv(jx_double((t->time_when_done - t->time_when_retrieval)/((double) ONE_SECOND)), jx_string("s"), NULL));
			jx_print_buffer(m, &B);
			jx_delete(m);
		} else {
			// no resources measured, one empty dictionary for limits broken, other for resources.
			buffer_printf(&B, " {} {}");
		}
	} else {
		struct vine_worker_info *w = t->worker;
		if(w) {
			buffer_printf(&B, " %s ", w->workerid);

			if(state == VINE_TASK_RUNNING) {
				const char *allocation = (t->resource_request == CATEGORY_ALLOCATION_FIRST ? "FIRST_RESOURCES" : "MAX_RESOURCES");
				buffer_printf(&B, " %s ", allocation);
				const struct rmsummary *box = itable_lookup(w->current_tasks_boxes, t->task_id);
				rmsummary_print_buffer(&B, box, 1);
			} else if(state == VINE_TASK_WAITING_RETRIEVAL) {
				/* do not add any info */
			}
		}
	}

	vine_txn_log_write(q, buffer_tostring(&B));
	buffer_free(&B);
}

void vine_txn_log_write_category(struct vine_manager *q, struct category *c)
{
	if(!q->txn_logfile)
		return;

	if(!c)
		return;

	struct buffer B;
	buffer_init(&B);

	buffer_printf(&B, "CATEGORY %s MAX ", c->name);
	rmsummary_print_buffer(&B, category_bucketing_dynamic_task_max_resources(c, NULL, CATEGORY_ALLOCATION_MAX, -1), 1);
	vine_txn_log_write(q, buffer_tostring(&B));
	buffer_rewind(&B, 0);

	buffer_printf(&B, "CATEGORY %s MIN ", c->name);
	rmsummary_print_buffer(&B, category_dynamic_task_min_resources(c, NULL, CATEGORY_ALLOCATION_FIRST), 1);
	vine_txn_log_write(q, buffer_tostring(&B));
	buffer_rewind(&B, 0);

	const char *mode;

	switch(c->allocation_mode) {
		case CATEGORY_ALLOCATION_MODE_MAX:
			mode = "MAX";
			break;
		case CATEGORY_ALLOCATION_MODE_MIN_WASTE:
			mode = "MIN_WASTE";
			break;
		case CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT:
			mode = "MAX_THROUGHPUT";
			break;
        case CATEGORY_ALLOCATION_MODE_GREEDY_BUCKETING:
            mode = "GREEDY_BUCKETING";
            break;
        case CATEGORY_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING:
            mode = "EXHAUSTIVE_BUCKETING";
            break;
		case CATEGORY_ALLOCATION_MODE_FIXED:
		default:
			mode = "FIXED";
			break;
	}

	buffer_printf(&B, "CATEGORY %s FIRST %s ", c->name, mode);
	rmsummary_print_buffer(&B, category_bucketing_dynamic_task_max_resources(c, NULL, CATEGORY_ALLOCATION_FIRST, -1), 1);
	vine_txn_log_write(q, buffer_tostring(&B));

	buffer_free(&B);
}

void vine_txn_log_write_worker(struct vine_manager *q, struct vine_worker_info *w, int leaving, vine_worker_disconnect_reason_t reason_leaving)
{
	struct buffer B;
	buffer_init(&B);

	buffer_printf(&B, "WORKER %s", w->workerid);

	if(leaving) {
		buffer_printf(&B, " DISCONNECTION");
		switch(reason_leaving) {
			case VINE_WORKER_DISCONNECT_IDLE_OUT:
				buffer_printf(&B, " IDLE_OUT");
				break;
			case VINE_WORKER_DISCONNECT_FAST_ABORT:
				buffer_printf(&B, " FAST_ABORT");
				break;
			case VINE_WORKER_DISCONNECT_FAILURE:
				buffer_printf(&B, " FAILURE");
				break;
			case VINE_WORKER_DISCONNECT_STATUS_WORKER:
				buffer_printf(&B, " STATUS_WORKER");
				break;
			case VINE_WORKER_DISCONNECT_EXPLICIT:
				buffer_printf(&B, " EXPLICIT");
				break;
			case VINE_WORKER_DISCONNECT_UNKNOWN:
			default:
				buffer_printf(&B, " UNKNOWN");
				break;
		}
	} else {
		buffer_printf(&B, " CONNECTION %s", w->addrport);
	}

	vine_txn_log_write(q, buffer_tostring(&B));

	buffer_free(&B);
}

void vine_txn_log_write_worker_resources(struct vine_manager *q, struct vine_worker_info *w)
{

	struct rmsummary *s = rmsummary_create(-1);

	s->cores  = w->resources->cores.total;
	s->memory = w->resources->memory.total;
	s->disk   = w->resources->disk.total;

	char *rjx = rmsummary_print_string(s, 1);


	struct buffer B;
	buffer_init(&B);

	buffer_printf(&B, "WORKER %s RESOURCES %s", w->workerid, rjx);

	vine_txn_log_write(q, buffer_tostring(&B));

	rmsummary_delete(s);
	buffer_free(&B);
	free(rjx);
}


void vine_txn_log_write_transfer(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, struct vine_file *f, size_t size_in_bytes, int time_in_usecs, int is_input )
{
	struct buffer B;
	buffer_init(&B);
	buffer_printf(&B, "WORKER %s TRANSFER ", w->workerid);
	buffer_printf(&B, is_input ? "INPUT":"OUTPUT");
	buffer_printf(&B, " %s", f->remote_name);
	buffer_printf(&B, " %f", size_in_bytes / ((double) MEGABYTE));
	buffer_printf(&B, " %f", time_in_usecs / ((double) USECOND));

	vine_txn_log_write(q, buffer_tostring(&B));
	buffer_free(&B);
}

void vine_txn_log_write_cache_update(struct vine_manager *q, struct vine_worker_info *w, size_t size_in_bytes, int time_in_usecs, const char *name )
{
	struct buffer B;

	buffer_init(&B);
	buffer_printf(&B, "WORKER %s CACHE_UPDATE", w->workerid);
	buffer_printf(&B, " %s", name);
	buffer_printf(&B, " %f", size_in_bytes / ((double) MEGABYTE));
	buffer_printf(&B, " %f", time_in_usecs / ((double) USECOND));

	vine_txn_log_write(q, buffer_tostring(&B));
	buffer_free(&B);
}

void vine_txn_log_write_manager(struct vine_manager *q, const char *event)
{
	struct buffer B;

	buffer_init(&B);
	buffer_printf(&B, "MANAGER %d %s", getpid(), event);
	vine_txn_log_write(q, buffer_tostring(&B));
	buffer_free(&B);
}



