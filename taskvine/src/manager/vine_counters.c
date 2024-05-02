
/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_counters.h"

#include "debug.h"

#include <stdio.h>

struct vine_counters vine_counters = {{0}, {0}, {0}, {0}, {0}};

static void vine_counter_print(const char *name, struct vine_counter *c)
{
	int nleaked = c->created + c->ref_added - c->deleted;
	if (nleaked == 0) {
		printf("%8s %8d %8d %8d ok", name, c->created, c->ref_added, c->deleted);
	} else {
		printf("%8s %8d %8d %8d leaked %d", name, c->created, c->ref_added, c->deleted, nleaked);
	}
}

void vine_counters_print()
{
	printf("  object  created   ref_added  deleted\n");
	printf("-----------------------------------\n");

	vine_counter_print("tasks", &vine_counters.task);
	vine_counter_print("mounts", &vine_counters.mount);
	vine_counter_print("files", &vine_counters.file);
	vine_counter_print("replicas", &vine_counters.replica);
	vine_counter_print("workers", &vine_counters.worker);
}

static void vine_counter_debug(const char *name, struct vine_counter *c)
{
	int nleaked = c->created + c->ref_added - c->deleted;
	if (nleaked == 0) {
		debug(D_VINE, "%8s %8d %8d %8d ok", name, c->created, c->ref_added, c->deleted);
	} else {
		debug(D_VINE, "%8s %8d %8d %8d leaked %d", name, c->created, c->ref_added, c->deleted, nleaked);
	}
}

void vine_counters_debug()
{
	debug(D_VINE, "  object  created   ref_added  deleted\n");
	debug(D_VINE, "-----------------------------------\n");

	vine_counter_debug("tasks", &vine_counters.task);
	vine_counter_debug("mounts", &vine_counters.mount);
	vine_counter_debug("files", &vine_counters.file);
	vine_counter_debug("replicas", &vine_counters.replica);
	vine_counter_debug("workers", &vine_counters.worker);
}
