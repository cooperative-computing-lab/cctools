/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_manager.h"

#include "buffer.h"
#include "debug.h"
#include "rmonitor_types.h"
#include "timestamp.h"
#include "vine_perf_log.h"

#include <stdio.h>
#include <unistd.h>

void vine_perf_log_write_header(struct vine_manager *q)
{
	setvbuf(q->perf_logfile, NULL, _IOLBF, 2048); // line buffered, we don't want incomplete lines
	fprintf(q->perf_logfile,
			// start with a comment
			"#"
			// time:
			" timestamp"
			// workers current:
			" workers_connected workers_init workers_idle workers_busy workers_able"
			// workers cumulative:
			" workers_joined workers_removed workers_released workers_idled_out workers_blocked workers_slow workers_lost"
			// tasks current:
			" tasks_waiting tasks_on_workers tasks_running tasks_with_results"
			// tasks cumulative
			" tasks_submitted tasks_dispatched tasks_done tasks_failed tasks_cancelled tasks_exhausted_attempts"
			// manager time statistics:
			" time_send time_receive time_send_good time_receive_good time_status_msgs time_internal time_polling time_application time_scheduling"
			// workers time statistics:
			" time_execute time_execute_good time_execute_exhaustion"
			// bandwidth:
			" bytes_sent bytes_received bandwidth"
			// resources:
			" capacity_tasks capacity_cores capacity_memory capacity_disk capacity_instantaneous capacity_weighted manager_load"
			" total_cores total_memory total_disk"
			" committed_cores committed_memory committed_disk"
			" max_cores max_memory max_disk"
			" min_cores min_memory min_disk"
			// end with a newline
			"\n");
}

void vine_perf_log_write_update(struct vine_manager *q, int force)
{
	struct vine_stats s;

	timestamp_t now = timestamp_get();
	if (!force && ((now - q->time_last_log_stats) < (long unsigned int)(ONE_SECOND * q->perf_log_interval))) {
		return;
	}

	vine_get_stats(q, &s);
	debug(D_VINE, "workers connections -- known: %d, connecting: %d", s.workers_connected, s.workers_init);

	q->time_last_log_stats = now;

	if (!q->perf_logfile) {
		return;
	}

	buffer_t B;
	buffer_init(&B);

	buffer_printf(&B, "%" PRIu64, timestamp_get());

	/* Stats for the current state of workers: */
	buffer_printf(&B, " %d", s.workers_connected);
	buffer_printf(&B, " %d", s.workers_init);
	buffer_printf(&B, " %d", s.workers_idle);
	buffer_printf(&B, " %d", s.workers_busy);
	buffer_printf(&B, " %d", s.workers_able);

	/* Cumulative stats for workers: */
	buffer_printf(&B, " %d", s.workers_joined);
	buffer_printf(&B, " %d", s.workers_removed);
	buffer_printf(&B, " %d", s.workers_released);
	buffer_printf(&B, " %d", s.workers_idled_out);
	buffer_printf(&B, " %d", s.workers_blocked);
	buffer_printf(&B, " %d", s.workers_slow);
	buffer_printf(&B, " %d", s.workers_lost);

	/* Stats for the current state of tasks: */
	buffer_printf(&B, " %d", s.tasks_waiting);
	buffer_printf(&B, " %d", s.tasks_on_workers);
	buffer_printf(&B, " %d", s.tasks_running);
	buffer_printf(&B, " %d", s.tasks_with_results);

	/* Cumulative stats for tasks: */
	buffer_printf(&B, " %d", s.tasks_submitted);
	buffer_printf(&B, " %d", s.tasks_dispatched);
	buffer_printf(&B, " %d", s.tasks_done);
	buffer_printf(&B, " %d", s.tasks_failed);
	buffer_printf(&B, " %d", s.tasks_cancelled);
	buffer_printf(&B, " %d", s.tasks_exhausted_attempts);

	/* Master time statistics: */
	buffer_printf(&B, " %" PRId64, s.time_send);
	buffer_printf(&B, " %" PRId64, s.time_receive);
	buffer_printf(&B, " %" PRId64, s.time_send_good);
	buffer_printf(&B, " %" PRId64, s.time_receive_good);
	buffer_printf(&B, " %" PRId64, s.time_status_msgs);
	buffer_printf(&B, " %" PRId64, s.time_internal);
	buffer_printf(&B, " %" PRId64, s.time_polling);
	buffer_printf(&B, " %" PRId64, s.time_application);
	buffer_printf(&B, " %" PRId64, s.time_scheduling);

	/* Workers time statistics: */
	buffer_printf(&B, " %" PRId64, s.time_workers_execute);
	buffer_printf(&B, " %" PRId64, s.time_workers_execute_good);
	buffer_printf(&B, " %" PRId64, s.time_workers_execute_exhaustion);

	/* BW statistics */
	buffer_printf(&B, " %" PRId64, s.bytes_sent);
	buffer_printf(&B, " %" PRId64, s.bytes_received);
	buffer_printf(&B, " %f", s.bandwidth);

	/* resources statistics */
	buffer_printf(&B, " %d", s.capacity_tasks);
	buffer_printf(&B, " %d", s.capacity_cores);
	buffer_printf(&B, " %d", s.capacity_memory);
	buffer_printf(&B, " %d", s.capacity_disk);
	buffer_printf(&B, " %d", s.capacity_instantaneous);
	buffer_printf(&B, " %d", s.capacity_weighted);
	buffer_printf(&B, " %f", s.manager_load);

	buffer_printf(&B, " %" PRId64, s.total_cores);
	buffer_printf(&B, " %" PRId64, s.total_memory);
	buffer_printf(&B, " %" PRId64, s.total_disk);

	buffer_printf(&B, " %" PRId64, s.committed_cores);
	buffer_printf(&B, " %" PRId64, s.committed_memory);
	buffer_printf(&B, " %" PRId64, s.committed_disk);

	buffer_printf(&B, " %" PRId64, s.max_cores);
	buffer_printf(&B, " %" PRId64, s.max_memory);
	buffer_printf(&B, " %" PRId64, s.max_disk);

	buffer_printf(&B, " %" PRId64, s.min_cores);
	buffer_printf(&B, " %" PRId64, s.min_memory);
	buffer_printf(&B, " %" PRId64, s.min_disk);

	fprintf(q->perf_logfile, "%s\n", buffer_tostring(&B));

	buffer_free(&B);
}
