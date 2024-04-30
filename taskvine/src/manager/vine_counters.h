/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_COUNTERS_H
#define VINE_COUNTERS_H

#include <stdint.h>

/*
For internal troubleshooting and profiling purposes, track the number of
creates/refaddes/deletes of objects of various types, so they can be
displayed at the end of a run.  vine_counters is a global object
that is access directly by vine_task_create/delete() and similar functions. 
*/

struct vine_counter {
	uint32_t created;
	uint32_t refadded;
	uint32_t deleted;
};

struct vine_counters {
	struct vine_counter task;
	struct vine_counter file;
	struct vine_counter replica;
	struct vine_counter mount;
	struct vine_counter worker;
};

extern struct vine_counters vine_counters;

/* Send performance counters to the debug log */
void vine_counters_debug();

/* Send performance counters to standard out. */
void vine_counters_print();

#endif
