/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef RMONITOR_POLL_H
#define RMONITOR_POLL_H

#include "rmsummary.h"

int rmonitor_measure_process(struct rmsummary *tr, pid_t pid);
int rmonitor_measure_process_update_to_peak(struct rmsummary *tr, pid_t pid);

#endif
