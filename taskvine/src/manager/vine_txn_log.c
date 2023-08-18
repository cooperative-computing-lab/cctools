/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_txn_log.h"
#include "vine_file.h"
#include "vine_task.h"
#include "vine_worker_info.h"

#include "buffer.h"
#include "jx.h"
#include "jx_print.h"
#include "macros.h"
#include "rmonitor_types.h"
#include "rmsummary.h"

#include <assert.h>
#include <stdio.h>
#include <unistd.h>

void vine_txn_log_write(struct vine_manager *q, const char *str)
{
	if (!q->txn_logfile)
		return;

	fprintf(q->txn_logfile, "%" PRIu64 " %d %s\n", timestamp_get(), getpid(), str);
	fflush(q->txn_logfile);
}

void vine_txn_log_write_header(struct vine_manager *q)
{
	setvbuf(q->txn_logfile, NULL, _IOLBF, 1024); // line buffered, we don't want incomplete lines

	fprintf(q->txn_logfile, "# time manager_pid MANAGER manager_pid START|END time_from_origin\n");
	fprintf(q->txn_logfile, "# time manager_pid WORKER worker_id CONNECTION host:port\n");
	fprintf(q->txn_logfile,
			"# time manager_pid WORKER worker_id DISCONNECTION (UNKNOWN|IDLE_OUT|FAST_ABORT|FAILURE|STATUS_WORKER|EXPLICIT)\n");
	fprintf(q->txn_logfile, "# time manager_pid WORKER worker_id RESOURCES {resources}\n");
	fprintf(q->txn_logfile,
			"# time manager_pid WORKER worker_id CACHE_UPDATE filename size_in_mb wall_time_us start_time_us\n");
	fprintf(q->txn_logfile,
			"# time manager_pid WORKER worker_id TRANSFER (INPUT|OUTPUT) filename size_in_mb wall_time_us start_time_us\n");
	fprintf(q->txn_logfile, "# time manager_pid CATEGORY name MAX {resources_max_per_task}\n");
	fprintf(q->txn_logfile, "# time manager_pid CATEGORY name MIN {resources_min_per_task_per_worker}\n");
	fprintf(q->txn_logfile,
			"# time manager_pid CATEGORY name FIRST (FIXED|MAX|MIN_WASTE|MAX_THROUGHPUT) {resources_requested}\n");
	fprintf(q->txn_logfile,
			"# time manager_pid TASK task_id WAITING category_name (FIRST_RESOURCES|MAX_RESOURCES) attempt_number {resources_requested}\n");
	fprintf(q->txn_logfile,
			"# time manager_pid TASK task_id RUNNING worker_id (FIRST_RESOURCES|MAX_RESOURCES) {resources_allocated}\n");
	fprintf(q->txn_logfile, "# time manager_pid TASK task_id WAITING_RETRIEVAL worker_id\n");
	fprintf(q->txn_logfile,
			"# time manager_pid TASK task_id RETRIEVED (SUCCESS|UNKNOWN|INPUT_MISSING|OUTPUT_MISSING|STDOUT_MISSING|SIGNAL|RESOURCE_EXHAUSTION|MAX_RETRIES|MAX_END_TIME|MAX_WALL_TIME|FORSAKEN) {limits_exceeded} {resources_measured}\n");
	fprintf(q->txn_logfile,
			"# time manager_pid TASK task_id DONE (SUCCESS|UNKNOWN|INPUT_MISSING|OUTPUT_MISSING|STDOUT_MISSING|SIGNAL|RESOURCE_EXHAUSTION|MAX_RETRIES|MAX_END_TIME|MAX_WALL_TIME|FORSAKEN) exit_code\n");
	fprintf(q->txn_logfile, "# time manager_pid LIBRARY library_id (WAITING|SENT|STARTED|FAILURE) worker_id");
	fprintf(q->txn_logfile, "\n");
}

static struct jx *resources_with_io_report(const struct vine_task *t, const struct rmsummary *s)
{

	struct jx *m = NULL;

	if (s) {
		m = rmsummary_to_json(s, /* only resources */ 1);
	} else {
		m = jx_object(0);
	}

	if (t->time_when_commit_start > 0) {
		/* if time_when_commit_start, then we now for sure the task was
		 * submitted to some worker, and that bytes sent makes sense. */

		jx_insert(m,
				jx_string("size_input_mgr"),
				jx_arrayv(jx_double(t->bytes_sent / ((double)MEGABYTE)), jx_string("MB"), NULL));
		jx_insert(m,
				jx_string("time_input_mgr"),
				jx_arrayv(jx_double((t->time_when_commit_end - t->time_when_commit_start) /
							  ((double)ONE_SECOND)),
						jx_string("s"),
						NULL));
		jx_insert(m,
				jx_string("time_commit_end"),
				jx_arrayv(jx_double(t->time_when_commit_end / ((double)ONE_SECOND)),
						jx_string("s"),
						NULL));
		jx_insert(m,
				jx_string("time_commit_start"),
				jx_arrayv(jx_double(t->time_when_commit_start / ((double)ONE_SECOND)),
						jx_string("s"),
						NULL));
	}

	if (t->time_when_retrieval > 0) {
		/* if time_when_retrieval, then information about execution is available */
		jx_insert(m,
				jx_string("size_output_mgr"),
				jx_arrayv(jx_double(t->bytes_received / ((double)MEGABYTE)), jx_string("MB"), NULL));
		jx_insert(m,
				jx_string("time_output_mgr"),
				jx_arrayv(jx_double((t->time_when_done - t->time_when_retrieval) /
							  ((double)ONE_SECOND)),
						jx_string("s"),
						NULL));
		jx_insert(m,
				jx_string("time_worker_end"),
				jx_arrayv(jx_double(t->time_workers_execute_last_end / ((double)ONE_SECOND)),
						jx_string("s"),
						NULL));
		jx_insert(m,
				jx_string("time_worker_start"),
				jx_arrayv(jx_double(t->time_workers_execute_last_start / ((double)ONE_SECOND)),
						jx_string("s"),
						NULL));
	}

	return m;
}

void vine_txn_log_write_task(struct vine_manager *q, struct vine_task *t)
{
	if (!q->txn_logfile)
		return;

	struct buffer B;
	buffer_init(&B);

	vine_task_state_t state = t->state;

	buffer_printf(&B, "TASK %d %s", t->task_id, vine_task_state_to_string(state));

	if (state == VINE_TASK_UNKNOWN) {
		/* do not add any info */
	} else if (state == VINE_TASK_READY) {
		const char *allocation = (t->resource_request == CATEGORY_ALLOCATION_FIRST ? "FIRST_RESOURCES"
											   : "MAX_RESOURCES");
		buffer_printf(&B, " %s %s %d ", t->category, allocation, t->try_count + 1);
		rmsummary_print_buffer(&B, vine_manager_task_resources_min(q, t), 1);
	} else if (state == VINE_TASK_CANCELED) {
		/* do not add any info */
	} else if (state == VINE_TASK_DONE) {
		buffer_printf(&B, " %s ", vine_result_string(t->result));
		buffer_printf(&B, " %d ", t->exit_code);
	} else if (state == VINE_TASK_RETRIEVED) {
		buffer_printf(&B, " %s ", vine_result_string(t->result));
		buffer_printf(&B, " %d ", t->exit_code);

		assert(t->resources_measured);
		if (t->result == VINE_RESULT_RESOURCE_EXHAUSTION) {
			rmsummary_print_buffer(&B, t->resources_measured->limits_exceeded, 1);
			buffer_printf(&B, " ");
		} else {
			// no limits broken, thus printing an empty dictionary
			buffer_printf(&B, " {} ");
		}

		struct jx *m = resources_with_io_report(t, t->resources_measured);
		jx_print_buffer(m, &B);
		jx_delete(m);
	} else {
		struct vine_worker_info *w = t->worker;
		if (w) {
			buffer_printf(&B, " %s ", w->workerid);

			if (state == VINE_TASK_RUNNING) {
				const char *allocation =
						(t->resource_request == CATEGORY_ALLOCATION_FIRST ? "FIRST_RESOURCES"
												  : "MAX_RESOURCES");
				buffer_printf(&B, " %s ", allocation);
				struct jx *m = resources_with_io_report(t, t->current_resource_box);
				jx_print_buffer(m, &B);
				jx_delete(m);
			} else if (state == VINE_TASK_WAITING_RETRIEVAL) {
				/* do not add any info */
			}
		}
	}

	vine_txn_log_write(q, buffer_tostring(&B));
	buffer_free(&B);
}

void vine_txn_log_write_category(struct vine_manager *q, struct category *c)
{
	if (!q->txn_logfile)
		return;

	if (!c)
		return;

	struct buffer B;
	buffer_init(&B);

	buffer_printf(&B, "CATEGORY %s MAX ", c->name);
	rmsummary_print_buffer(&B, category_task_max_resources(c, NULL, CATEGORY_ALLOCATION_MAX, -1), 1);
	vine_txn_log_write(q, buffer_tostring(&B));
	buffer_rewind(&B, 0);

	buffer_printf(&B, "CATEGORY %s MIN ", c->name);
	rmsummary_print_buffer(&B, category_task_min_resources(c, NULL, CATEGORY_ALLOCATION_FIRST, -1), 1);
	vine_txn_log_write(q, buffer_tostring(&B));
	buffer_rewind(&B, 0);

	const char *mode;

	switch (c->allocation_mode) {
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
	rmsummary_print_buffer(&B, category_task_max_resources(c, NULL, CATEGORY_ALLOCATION_FIRST, -1), 1);
	vine_txn_log_write(q, buffer_tostring(&B));

	buffer_free(&B);
}

void vine_txn_log_write_worker(struct vine_manager *q, struct vine_worker_info *w, int leaving,
		vine_worker_disconnect_reason_t reason_leaving)
{
	if (reason_leaving == VINE_WORKER_DISCONNECT_STATUS_WORKER) {
		/* status worker should not generate a transaction */
		return;
	}

	struct buffer B;
	buffer_init(&B);

	buffer_printf(&B, "WORKER %s", w->workerid);

	if (leaving) {
		buffer_printf(&B, " DISCONNECTION");
		switch (reason_leaving) {
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

	s->cores = w->resources->cores.total;
	s->memory = w->resources->memory.total;
	s->disk = w->resources->disk.total;

	char *rjx = rmsummary_print_string(s, 1);

	struct buffer B;
	buffer_init(&B);

	buffer_printf(&B, "WORKER %s RESOURCES %s", w->workerid, rjx);

	vine_txn_log_write(q, buffer_tostring(&B));

	rmsummary_delete(s);
	buffer_free(&B);
	free(rjx);
}

void vine_txn_log_write_transfer(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t,
		struct vine_mount *m, struct vine_file *f, size_t size_in_bytes, timestamp_t time_in_usecs,
		timestamp_t start_in_usecs, int is_input)
{
	struct buffer B;
	buffer_init(&B);
	buffer_printf(&B, "WORKER %s TRANSFER ", w->workerid);
	buffer_printf(&B, is_input ? "INPUT" : "OUTPUT");
	buffer_printf(&B, " %s", m->remote_name);
	buffer_printf(&B, " %lld", (long long)size_in_bytes);
	buffer_printf(&B, " %llu", (unsigned long long)time_in_usecs);
	buffer_printf(&B, " %llu", (unsigned long long)start_in_usecs);

	vine_txn_log_write(q, buffer_tostring(&B));
	buffer_free(&B);
}

void vine_txn_log_write_cache_update(struct vine_manager *q, struct vine_worker_info *w, size_t size_in_bytes,
		timestamp_t time_in_usecs, timestamp_t start_in_usecs, const char *name)
{
	struct buffer B;

	buffer_init(&B);
	buffer_printf(&B, "WORKER %s CACHE_UPDATE", w->workerid);
	buffer_printf(&B, " %s", name);
	buffer_printf(&B, " %lld", (long long)size_in_bytes);
	buffer_printf(&B, " %llu", (unsigned long long)time_in_usecs);
	buffer_printf(&B, " %llu", (unsigned long long)start_in_usecs);

	vine_txn_log_write(q, buffer_tostring(&B));
	buffer_free(&B);
}

void vine_txn_log_write_manager(struct vine_manager *q, const char *event)
{
	struct buffer B;
	int64_t time_from_origin = 0;

	if (strcmp("START", event)) {
		time_from_origin = timestamp_get() - q->stats->time_when_started;
	}

	buffer_init(&B);
	buffer_printf(&B, "MANAGER %d %s %" PRId64, getpid(), event, time_from_origin);
	vine_txn_log_write(q, buffer_tostring(&B));
	buffer_free(&B);
}

void vine_txn_log_write_library_update(
		struct vine_manager *q, struct vine_worker_info *w, int library_id, vine_library_state_t state)
{
	struct buffer B;
	buffer_init(&B);
	buffer_printf(&B, "LIBRARY %d", library_id);

	const char *status = "UNKNOWN";
	switch (state) {
	case VINE_LIBRARY_WAITING:
		status = "WAITING";
		break;
	case VINE_LIBRARY_SENT:
		status = "SENT";
		break;
	case VINE_LIBRARY_STARTED:
		status = "STARTED";
		break;
	case VINE_LIBRARY_FAILURE:
		status = "FAILURE";
		break;
	}

	buffer_printf(&B, " %s", status);
	buffer_printf(&B, " %s", w->workerid);

	vine_txn_log_write(q, buffer_tostring(&B));
	buffer_free(&B);
}

/* vim: set noexpandtab tabstop=8: */
