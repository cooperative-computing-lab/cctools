
#include "ds_transaction.h"
#include "ds_worker_info.h"
#include "ds_task.h"
#include "ds_file.h"

#include "buffer.h"
#include "rmsummary.h"
#include "macros.h"
#include "jx.h"
#include "jx_print.h"
#include "rmsummary.h"
#include "rmonitor_types.h"

#include <stdio.h>
#include <unistd.h>

void ds_transaction_write_header( struct ds_manager *q )
{
	setvbuf(q->transactions_logfile, NULL, _IOLBF, 1024); // line buffered, we don't want incomplete lines

	fprintf(q->transactions_logfile, "# time manager_pid MANAGER START|END\n");
	fprintf(q->transactions_logfile, "# time manager_pid WORKER worker_id host:port CONNECTION\n");
	fprintf(q->transactions_logfile, "# time manager_pid WORKER worker_id host:port DISCONNECTION (UNKNOWN|IDLE_OUT|FAST_ABORT|FAILURE|STATUS_WORKER|EXPLICIT\n");
	fprintf(q->transactions_logfile, "# time manager_pid WORKER worker_id RESOURCES {resources}\n");
	fprintf(q->transactions_logfile, "# time manager_pid CATEGORY name MAX {resources_max_per_task}\n");
	fprintf(q->transactions_logfile, "# time manager_pid CATEGORY name MIN {resources_min_per_task_per_worker}\n");
	fprintf(q->transactions_logfile, "# time manager_pid CATEGORY name FIRST (FIXED|MAX|MIN_WASTE|MAX_THROUGHPUT) {resources_requested}\n");
	fprintf(q->transactions_logfile, "# time manager_pid TASK taskid WAITING category_name (FIRST_RESOURCES|MAX_RESOURCES) {resources_requested}\n");
	fprintf(q->transactions_logfile, "# time manager_pid TASK taskid RUNNING worker_address (FIRST_RESOURCES|MAX_RESOURCES) {resources_allocated}\n");
	fprintf(q->transactions_logfile, "# time manager_pid TASK taskid WAITING_RETRIEVAL worker_address\n");
	fprintf(q->transactions_logfile, "# time manager_pid TASK taskid (RETRIEVED|DONE) (SUCCESS|SIGNAL|END_TIME|FORSAKEN|MAX_RETRIES|MAX_WALLTIME|UNKNOWN|RESOURCE_EXHAUSTION) exit_code {limits_exceeded} {resources_measured}\n");
	fprintf(q->transactions_logfile, "# time manager_pid TRANSFER (INPUT|OUTPUT) taskid cache_flag sizeinmb walltime filename\n");
	fprintf(q->transactions_logfile, "\n");
}

void ds_transaction_write(struct ds_manager *q, const char *str)
{
	if(!q->transactions_logfile)
		return;

	fprintf(q->transactions_logfile, "%" PRIu64, timestamp_get());
	fprintf(q->transactions_logfile, " %d", getpid());
	fprintf(q->transactions_logfile, " %s", str);
	fprintf(q->transactions_logfile, "\n");
}

void ds_transaction_write_task(struct ds_manager *q, struct ds_task *t)
{
	if(!q->transactions_logfile)
		return;

	struct buffer B;
	buffer_init(&B);

	ds_task_state_t state = (uintptr_t) itable_lookup(q->task_state_map, t->taskid);

	buffer_printf(&B, "TASK %d %s", t->taskid, ds_task_state_string(state));

	if(state == DS_TASK_UNKNOWN) {
			/* do not add any info */
	} else if(state == DS_TASK_READY) {
		const char *allocation = (t->resource_request == CATEGORY_ALLOCATION_FIRST ? "FIRST_RESOURCES" : "MAX_RESOURCES");
		buffer_printf(&B, " %s %s ", t->category, allocation);
		rmsummary_print_buffer(&B, task_min_resources(q, t), 1);
	} else if(state == DS_TASK_CANCELED) {
			/* do not add any info */
	} else if(state == DS_TASK_RETRIEVED || state == DS_TASK_DONE) {
		buffer_printf(&B, " %s ", ds_result_str(t->result));
		buffer_printf(&B, " %d ", t->exit_code);

		if(t->resources_measured) {
			if(t->result == DS_RESULT_RESOURCE_EXHAUSTION) {
				rmsummary_print_buffer(&B, t->resources_measured->limits_exceeded, 1);
				buffer_printf(&B, " ");
			}
			else {
				// no limits broken, thus printing an empty dictionary
				buffer_printf(&B, " {} ");
			}

			struct jx *m = rmsummary_to_json(t->resources_measured, /* only resources */ 1);
			jx_insert(m, jx_string("ds_input_size"), jx_arrayv(jx_double(t->bytes_sent/((double) MEGABYTE)), jx_string("MB"), NULL));
			jx_insert(m, jx_string("ds_output_size"), jx_arrayv(jx_double(t->bytes_received/((double) MEGABYTE)), jx_string("MB"), NULL));
			jx_insert(m, jx_string("ds_input_time"), jx_arrayv(jx_double((t->time_when_commit_end - t->time_when_commit_start)/((double) ONE_SECOND)), jx_string("s"), NULL));
			jx_insert(m, jx_string("ds_output_time"), jx_arrayv(jx_double((t->time_when_done - t->time_when_retrieval)/((double) ONE_SECOND)), jx_string("s"), NULL));
			jx_print_buffer(m, &B);
			jx_delete(m);
		} else {
			// no resources measured, one empty dictionary for limits broken, other for resources.
			buffer_printf(&B, " {} {}");
		}
	} else {
		struct ds_worker_info *w = itable_lookup(q->worker_task_map, t->taskid);
		const char *worker_str = "worker-info-not-available";

		if(w) {
			worker_str = w->addrport;
			buffer_printf(&B, " %s ", worker_str);

			if(state == DS_TASK_RUNNING) {
				const char *allocation = (t->resource_request == CATEGORY_ALLOCATION_FIRST ? "FIRST_RESOURCES" : "MAX_RESOURCES");
				buffer_printf(&B, " %s ", allocation);
				const struct rmsummary *box = itable_lookup(w->current_tasks_boxes, t->taskid);
				rmsummary_print_buffer(&B, box, 1);
			} else if(state == DS_TASK_WAITING_RETRIEVAL) {
				/* do not add any info */
			}
		}
	}

	ds_transaction_write(q, buffer_tostring(&B));
	buffer_free(&B);
}

void ds_transaction_write_category(struct ds_manager *q, struct category *c)
{
	if(!q->transactions_logfile)
		return;

	if(!c)
		return;

	struct buffer B;
	buffer_init(&B);

	buffer_printf(&B, "CATEGORY %s MAX ", c->name);
	rmsummary_print_buffer(&B, category_dynamic_task_max_resources(c, NULL, CATEGORY_ALLOCATION_MAX), 1);
	ds_transaction_write(q, buffer_tostring(&B));
	buffer_rewind(&B, 0);

	buffer_printf(&B, "CATEGORY %s MIN ", c->name);
	rmsummary_print_buffer(&B, category_dynamic_task_min_resources(c, NULL, CATEGORY_ALLOCATION_FIRST), 1);
	ds_transaction_write(q, buffer_tostring(&B));
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
		case CATEGORY_ALLOCATION_MODE_FIXED:
		default:
			mode = "FIXED";
			break;
	}

	buffer_printf(&B, "CATEGORY %s FIRST %s ", c->name, mode);
	rmsummary_print_buffer(&B, category_dynamic_task_max_resources(c, NULL, CATEGORY_ALLOCATION_FIRST), 1);
	ds_transaction_write(q, buffer_tostring(&B));

	buffer_free(&B);
}

void ds_transaction_write_worker(struct ds_manager *q, struct ds_worker_info *w, int leaving, ds_worker_disconnect_reason_t reason_leaving)
{
	struct buffer B;
	buffer_init(&B);

	buffer_printf(&B, "WORKER %s %s ", w->workerid, w->addrport);

	if(leaving) {
		buffer_printf(&B, " DISCONNECTION");
		switch(reason_leaving) {
			case DS_WORKER_DISCONNECT_IDLE_OUT:
				buffer_printf(&B, " IDLE_OUT");
				break;
			case DS_WORKER_DISCONNECT_FAST_ABORT:
				buffer_printf(&B, " FAST_ABORT");
				break;
			case DS_WORKER_DISCONNECT_FAILURE:
				buffer_printf(&B, " FAILURE");
				break;
			case DS_WORKER_DISCONNECT_STATUS_WORKER:
				buffer_printf(&B, " STATUS_WORKER");
				break;
			case DS_WORKER_DISCONNECT_EXPLICIT:
				buffer_printf(&B, " EXPLICIT");
				break;
			case DS_WORKER_DISCONNECT_UNKNOWN:
			default:
				buffer_printf(&B, " UNKNOWN");
				break;
		}
	} else {
		buffer_printf(&B, " CONNECTION");
	}

	ds_transaction_write(q, buffer_tostring(&B));

	buffer_free(&B);
}

void ds_transaction_write_worker_resources(struct ds_manager *q, struct ds_worker_info *w)
{

	struct rmsummary *s = rmsummary_create(-1);

	s->cores  = w->resources->cores.total;
	s->memory = w->resources->memory.total;
	s->disk   = w->resources->disk.total;

	char *rjx = rmsummary_print_string(s, 1);


	struct buffer B;
	buffer_init(&B);

	buffer_printf(&B, "WORKER %s RESOURCES %s", w->workerid, rjx);

	ds_transaction_write(q, buffer_tostring(&B));

	rmsummary_delete(s);
	buffer_free(&B);
	free(rjx);
}


void ds_transaction_write_transfer(struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, struct ds_file *f, size_t size_in_bytes, int time_in_usecs, ds_file_type_t type)
{
	struct buffer B;
	buffer_init(&B);
	buffer_printf(&B, "TRANSFER ");
	buffer_printf(&B, type == DS_INPUT ? "INPUT":"OUTPUT");
	buffer_printf(&B, " %d", t->taskid);
	buffer_printf(&B, " %d", f->flags & DS_CACHE);
	buffer_printf(&B, " %f", size_in_bytes / ((double) MEGABYTE));
	buffer_printf(&B, " %f", time_in_usecs / ((double) USECOND));
	buffer_printf(&B, " %s", f->remote_name);

	ds_transaction_write(q, buffer_tostring(&B));
	buffer_free(&B);
}


