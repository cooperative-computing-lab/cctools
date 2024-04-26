
/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_counters.h"

#include "debug.h"

#include <stdio.h>

struct vine_counters vine_counters = {{0},{0},{0},{0},{0}};

static void vine_counter_print(const char *name, struct vine_counter *c)
{
	printf("%8s %8d %8d %8d ", name, c->create, c->clone, c->delete);
	int nleaked = c->create + c->clone - c->delete;
	if (nleaked == 0) {
		printf("ok\n");
	} else {
		printf("leaked %d\n", nleaked);
	}
}

void vine_counters_print()
{
	printf("  object  created   cloned  deleted\n");
	printf("-----------------------------------\n");

	vine_counter_print("tasks", &vine_counters.task);
	vine_counter_print("mounts", &vine_counters.mount);
	vine_counter_print("files", &vine_counters.file);
	vine_counter_print("replicas", &vine_counters.replica);
	vine_counter_print("workers", &vine_counters.worker);
}

static void vine_counter_debug(const char *name, struct vine_counter *c)
{
	debug(D_VINE, "%8s %8d %8d %8d ", name, c->create, c->clone, c->delete);
	int nleaked = c->create + c->clone - c->delete;
	if (nleaked == 0) {
		debug(D_VINE, "ok\n");
	} else {
		debug(D_VINE, "leaked %d\n", nleaked);
	}
}

void vine_counters_debug()
{
	debug(D_VINE, "  object  created   cloned  deleted\n");
	debug(D_VINE, "-----------------------------------\n");

	vine_counter_debug("tasks", &vine_counters.task);
	vine_counter_debug("mounts", &vine_counters.mount);
	vine_counter_debug("files", &vine_counters.file);
	vine_counter_debug("replicas", &vine_counters.replica);
	vine_counter_debug("workers", &vine_counters.worker);
}
