/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue_catalog.h"

#include "work_queue.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "datagram.h"
#include "domain_name_cache.h"
#include "nvpair.h"
#include "hash_cache.h"
#include "list.h"
#include "xmalloc.h"
#include "debug.h"
#include "stringtools.h"
#include "domain_name_cache.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>


//static char *catalog_server_host = NULL;
//static int catalog_server_port = 0;
static struct datagram *outgoing_datagram = NULL;

int parse_catalog_server_description(char *server_string, char **host, int *port)
{
	char *colon;

	colon = strchr(server_string, ':');

	if(!colon) {
		*host = NULL;
		*port = 0;
		return 0;
	}

	*colon = '\0';

	*host = strdup(server_string);
	*port = atoi(colon + 1);

	// if (*port) == 0, parsing failed, thus return 0
	return *port;
}

struct work_queue_master *parse_work_queue_master_nvpair(struct nvpair *nv)
{
	struct work_queue_master *m;

	m = xxmalloc(sizeof(struct work_queue_master));
	if(!m) return NULL;

	strncpy(m->addr, nvpair_lookup_string(nv, "address"), LINK_ADDRESS_MAX);
	strncpy(m->proj, nvpair_lookup_string(nv, "project"), WORK_QUEUE_NAME_MAX);
	m->port = nvpair_lookup_integer(nv, "port");
	m->priority = nvpair_lookup_integer(nv, "priority");
	if(m->priority < 0) m->priority = 0;
	m->capacity = nvpair_lookup_integer(nv, "capacity");
	m->tasks_waiting = nvpair_lookup_integer(nv, "tasks_waiting");
	m->tasks_running = nvpair_lookup_integer(nv, "tasks_running");
	m->tasks_complete = nvpair_lookup_integer(nv, "tasks_complete");
	m->total_tasks_dispatched = nvpair_lookup_integer(nv, "total_tasks_dispatched");
	m->workers_init = nvpair_lookup_integer(nv, "workers_init");
	m->workers_ready = nvpair_lookup_integer(nv, "workers_ready");
	m->workers_busy = nvpair_lookup_integer(nv, "workers_busy");
	m->workers = nvpair_lookup_integer(nv, "workers");

	const char *owner;
	owner = nvpair_lookup_string(nv, "owner");
	if(owner) {
		strncpy(m->owner, nvpair_lookup_string(nv, "owner"), USERNAME_MAX);
	} else {
		strncpy(m->owner, "unknown", USERNAME_MAX);
	}

	return m;
}

struct work_queue_master *duplicate_work_queue_master(struct work_queue_master *master)
{
	struct work_queue_master *m;

	m = xxmalloc(sizeof(struct work_queue_master));
	if(!m) return NULL;
	strncpy(m->addr, master->addr, LINK_ADDRESS_MAX);
	strncpy(m->proj, master->proj, WORK_QUEUE_NAME_MAX);
	m->port = master->port;
	m->priority = master->priority;
	m->capacity = master->capacity;
	m->tasks_waiting = master->tasks_waiting; 
	m->tasks_running = master->tasks_running; 
	m->tasks_complete = master-> tasks_complete; 
	m->total_tasks_dispatched = master-> total_tasks_dispatched; 
	m->workers_init = master->workers_init; 
	m->workers_ready = master->workers_ready; 
	m->workers_busy = master->workers_busy; 
	m->workers = master->workers; 

	return m;
}

struct list *get_masters_from_catalog(const char *catalog_host, int catalog_port, struct list *regex_list)
{
	struct catalog_query *q;
	struct nvpair *nv;
	struct list *ml;
	struct work_queue_master *m;
	char *regex;
	time_t timeout = 60, stoptime;

	stoptime = time(0) + timeout;

	q = catalog_query_create(catalog_host, catalog_port, stoptime);
	if(!q) {
		fprintf(stderr, "Failed to query catalog server at %s:%d\n", catalog_host, catalog_port);
		return NULL;
	}

	ml = list_create();
	if(!ml)
		return NULL;

	while((nv = catalog_query_read(q, stoptime))) {
		if(strcmp(nvpair_lookup_string(nv, "type"), CATALOG_TYPE_WORK_QUEUE_MASTER) == 0) {
			m = parse_work_queue_master_nvpair(nv);
			if(m) {
				if(regex_list) {
					// Matched preferred masters
					list_first_item(regex_list);
					while((regex = (char *)list_next_item(regex_list))) {
						if(whole_string_match_regex(m->proj, regex)) {
							debug(D_WQ, "Master matched: %s -> %s\n", regex, m->proj);
							list_push_head(ml, m);
						}
					}
				} else {
					list_push_head(ml, m);
				}
			} else {
				fprintf(stderr, "Failed to parse a work queue master record!\n");
			}
		}
		nvpair_delete(nv);
	}

	// Must delete the query otherwise it would occupy 1 tcp connection forever!
	catalog_query_delete(q);
	return ml;
}

int advertise_master_to_catalog(const char *catalog_host, int catalog_port, const char *project_name, struct work_queue_stats *s)
{
	char address[DATAGRAM_ADDRESS_MAX];
	char owner[USERNAME_MAX];
	static char text[WORK_QUEUE_CATALOG_LINE_MAX];
	static time_t last_update_time = 0;

	if(time(0) - last_update_time < WORK_QUEUE_CATALOG_UPDATE_INTERVAL) return 1;

    if(!outgoing_datagram) {
        outgoing_datagram = datagram_create(0);
        if(!outgoing_datagram)
            fprintf(stderr, "Couldn't create outgoing udp port, thus work queue master info won't be sent to the catalog server!");
            return 0;
    }

	if(!username_get(owner)) {
		strcpy(owner,"unknown");
	}

	snprintf(text, WORK_QUEUE_CATALOG_LINE_MAX, "type wq_master\nproject %s\npriority %d\nport %d\nlifetime %d\ntasks_waiting %d\ntasks_complete %d\ntask_running%d\ntotal_tasks_dispatched %d\nworkers_init %d\nworkers_ready %d\nworkers_busy %d\nworkers %d\ncapacity %d\nversion %d.%d.%d\nowner %s", project_name, s->priority, s->port, WORK_QUEUE_CATALOG_LIFETIME, s->tasks_waiting, s->total_tasks_complete, s->tasks_running, s->total_tasks_dispatched, s->workers_init, s->workers_ready, s->workers_busy, s->workers_init + s->workers_ready + s->workers_busy, s->capacity, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, owner);

	if(domain_name_cache_lookup(catalog_host, address)) {
		debug(D_WQ, "Sending the master information to the catalog server at %s:%d ...", catalog_host, catalog_port);
		datagram_send(outgoing_datagram, text, strlen(text), address, catalog_port);
	}

	last_update_time = time(0);
	return 1;
}

void debug_print_masters(struct list *ml)
{
	struct work_queue_master *m;
	int count = 0;

	list_first_item(ml);
	while((m = (struct work_queue_master *) list_next_item(ml))) {
		debug(D_WQ, "Master %d:\n", ++count);
		debug(D_WQ, "addr:\t%s\n", m->addr);
		debug(D_WQ, "port:\t%d\n", m->port);
		debug(D_WQ, "project:\t%s\n", m->proj);
		debug(D_WQ, "priority:\t%d\n", m->priority);
		debug(D_WQ, "capacity:\t%d\n", m->capacity);
		debug(D_WQ, "\n");
	}
}
