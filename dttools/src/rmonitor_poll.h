/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef RMONITOR_POLL_H
#define RMONITOR_POLL_H

#include "rmsummary.h"

struct rmsummary *rmonitor_measure_process(pid_t pid);
int rmonitor_measure_process_update_to_peak(struct rmsummary *tr, pid_t pid);
struct rmsummary *rmonitor_measure_host(char *);

int rmonitor_get_children(pid_t pid, uint64_t **children);


typedef enum {
	MINIMONITOR_RESET    = 0,
	MINIMONITOR_ADD_PID,
	MINIMONITOR_REMOVE_PID,
	MINIMONITOR_MEASURE
} minimonitor_op;

struct rmsummary *rmonitor_minimonitor(minimonitor_op op, uint64_t pid);

#endif
