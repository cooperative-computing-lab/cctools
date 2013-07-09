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
#include "xxmalloc.h"
#include "debug.h"
#include "stringtools.h"
#include "domain_name_cache.h"
#include "timestamp.h"
#include "buffer.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

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

	*colon = ':';

	// if (*port) == 0, parsing failed, thus return 0
	return *port;
}

struct work_queue_pool *parse_work_queue_pool_nvpair(struct nvpair *nv)
{
	struct work_queue_pool *p;

	p = xxmalloc(sizeof(*p));

	strncpy(p->addr, nvpair_lookup_string(nv, "address"), LINK_ADDRESS_MAX);
	strncpy(p->name, nvpair_lookup_string(nv, "pool_name"), WORK_QUEUE_POOL_NAME_MAX);
	p->decision = xxstrdup(nvpair_lookup_string(nv, "decision"));
	strncpy(p->owner, nvpair_lookup_string(nv, "owner"), USERNAME_MAX);

	return p;
}

void free_work_queue_pool(struct work_queue_pool *p) {
	if(!p) return;

	// free dynamically allocated memory
	free(p->decision);

	free(p);
}

// workers_by_item format: "item1_name:item1_value, item2_name:item2_value, ..."
// returns the item value of item - "item_name"
int workers_by_item(const char *workers_by_item, const char *item_name) {
	if(!workers_by_item || !item_name) {
		return -1;
	}

	char *wbi, *item, *eq;

	wbi = xxstrdup(workers_by_item);
	item = strtok(wbi, " \t,"); 
	while(item) {
		eq = strchr(item, ':');
		if(eq) {
			int n;

			*eq = '\0';
			if(!strncmp(item, item_name, WORK_QUEUE_LINE_MAX)) {
				n = atoi(eq+1);
				if(n < 0) {
					*eq = '=';
					fprintf(stderr, "Number of workers in item \"%s\" is invalid.\n", item);
					break;
				} else {
					free(wbi);
					return n;
				}
			} 

			*eq = ':';
		} else {
			if(!strncmp(item, "n/a", 3)) {
				break;
			} else {
				fprintf(stderr, "Invalid worker distribution item: \"%s\".\n", item);
				break;
			}
		}
		item = strtok(0, " \t,");
	}
	free(wbi);
	return -1;
}

struct work_queue_master *parse_work_queue_master_nvpair(struct nvpair *nv)
{
	struct work_queue_master *m;

	m = xxmalloc(sizeof(struct work_queue_master));

	strncpy(m->addr, nvpair_lookup_string(nv, "address"), LINK_ADDRESS_MAX);

	const char *project;
	project = nvpair_lookup_string(nv, "project");
	if(project) {
		strncpy(m->proj, project, WORK_QUEUE_NAME_MAX);
	} else {
		strncpy(m->proj, "unknown", WORK_QUEUE_NAME_MAX);
	}

	m->port = nvpair_lookup_integer(nv, "port");
	m->start_time = nvpair_lookup_integer(nv, "starttime");
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
	m->workers_full = nvpair_lookup_integer(nv, "workers_full");
	m->workers = nvpair_lookup_integer(nv, "workers");

	const char *workers_by_pool;
	workers_by_pool = nvpair_lookup_string(nv, "workers_by_pool");

	if(workers_by_pool) {
		m->workers_by_pool = xxstrdup(workers_by_pool);
	} else {
		m->workers_by_pool = xxstrdup("unknown");
	}

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
	m->workers_full = master->workers_full; 
	m->workers = master->workers; 

	if(master->workers_by_pool) {
		m->workers_by_pool = xxstrdup(master->workers_by_pool);
	} else {
		m->workers_by_pool = NULL;
	}

	strncpy(m->owner, master->owner, USERNAME_MAX);

	return m;
}


void free_work_queue_master(struct work_queue_master *m) {
	if(!m) return;

	// free dynamically allocated memory
	free(m->workers_by_pool);

	free(m);
}

void free_work_queue_master_list(struct list *ml) {
	if(!ml) return;

	struct work_queue_master *m;

	list_first_item(ml);
	while((m = (struct work_queue_master *)list_next_item(ml))) {
		free_work_queue_master(m);
	}

	list_delete(ml);
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
					int match_found = 0;
					list_first_item(regex_list);
					while((regex = (char *)list_next_item(regex_list))) {
						if(whole_string_match_regex(m->proj, regex)) {
							debug(D_WQ, "Master matched: %s -> %s\n", regex, m->proj);
							list_push_head(ml, m);
							match_found = 1;
							break;
						}
					}
					if(match_found == 0) {
						free_work_queue_master(m);
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

int advertise_master_to_catalog(const char *catalog_host, int catalog_port, const char *project_name, const char *master_address, struct work_queue_stats *s, struct work_queue_resources *r, const char *workers_by_pool ) {
	char address[DATAGRAM_ADDRESS_MAX];
	char owner[USERNAME_MAX];
	buffer_t *buffer = NULL;
	const char *text;
	size_t text_size;

	if(!outgoing_datagram) {
		outgoing_datagram = datagram_create(0);
		if(!outgoing_datagram) {
			fprintf(stderr, "Failed to advertise master to catalog server: couldn't create outgoing udp datagram!\n");
			return 0;
		}
	}

	if(!username_get(owner)) {
		strcpy(owner,"unknown");
	}

	buffer = buffer_create();

	int total_workers_working = s->workers_busy + s->workers_full;
	int total_workers         = total_workers_working + s->workers_ready;

	buffer_printf(buffer, 
			"type wq_master\n"
			"project %s\nstarttime %llu\npriority %d\n"
			"port %d\nlifetime %d\n"
			"tasks_waiting %d\ntasks_complete %d\ntasks_running %d\ntotal_tasks_dispatched %d\n"
			"workers_init %d\nworkers_ready %d\nworkers_busy %d\nworkers %d\nworkers_by_pool %s\n"
			"cores_total %d\nmemory_total %d\ndisk_total %d\n"
			"capacity %d\n"
			"my_master %s\n"
			"version %d.%d.%s\nowner %s", 
			project_name, (s->start_time)/1000000, s->priority, 
			s->port, WORK_QUEUE_CATALOG_MASTER_AD_LIFETIME, 
			s->tasks_waiting, s->total_tasks_complete, s->tasks_running, s->total_tasks_dispatched, 
			s->workers_init, s->workers_ready, total_workers_working, total_workers, workers_by_pool, 
			r->cores.total, r->memory.total, r->disk.total,
			s->capacity, 
			master_address,
			CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, owner);

	text = buffer_tostring(buffer, &text_size);

	if(domain_name_cache_lookup(catalog_host, address)) {
		debug(D_WQ, "Advertising master status to the catalog server at %s:%d ...", catalog_host, catalog_port);
		datagram_send(outgoing_datagram, text, strlen(text), address, catalog_port);
	}

	buffer_delete(buffer);

	return 1;
}

int get_pool_decisions_from_catalog(const char *catalog_host, int catalog_port, const char *proj, struct list *decisions) {
	struct catalog_query *q;
	struct nvpair *nv;
	time_t timeout = 60, stoptime;

	stoptime = time(0) + timeout;

	if(!decisions) {
		fprintf(stderr, "No list to store pool decisions.\n");
		return 0;
	}

	q = catalog_query_create(catalog_host, catalog_port, stoptime);
	if(!q) {
		fprintf(stderr, "Failed to query catalog server at %s:%d\n", catalog_host, catalog_port);
		return 0;
	}
	

	// multiple pools
	while((nv = catalog_query_read(q, stoptime))) {
		if(strcmp(nvpair_lookup_string(nv, "type"), CATALOG_TYPE_WORK_QUEUE_POOL) == 0) {
			struct work_queue_pool *p;
			p = parse_work_queue_pool_nvpair(nv);
			debug(D_WQ, "pool %s's decision: %s\n", p->name, p->decision);
			int x = workers_by_item(p->decision, proj);
			if(x >= 0) {
				struct pool_info *pi;
				pi = (struct pool_info *)xxmalloc(sizeof(*pi));
				strncpy(pi->name, p->name, WORK_QUEUE_POOL_NAME_MAX);
				pi->count = x;
				list_push_tail(decisions, pi);
			}
			free(p->decision);
			free(p);
			
		}
		nvpair_delete(nv);
	}

	// Must delete the query otherwise it would occupy 1 tcp connection forever!
	catalog_query_delete(q);
	return 1;
}

int advertise_pool_decision_to_catalog(const char *catalog_host, int catalog_port, const char *pool_name, pid_t pid, time_t start_time, const char *decision, int workers_requested)
{
	char address[DATAGRAM_ADDRESS_MAX];
	char owner[USERNAME_MAX];
	buffer_t *buffer = NULL;
	const char *text;
	size_t text_size;

	static time_t last_update_time = 0;

	if(time(0) - last_update_time < WORK_QUEUE_CATALOG_POOL_UPDATE_INTERVAL) return 1;

	if(!outgoing_datagram) {
		outgoing_datagram = datagram_create(0);
		if(!outgoing_datagram) {
			fprintf(stderr, "Couldn't create outgoing udp port, thus work queue master info won't be sent to the catalog server!\n");
			return 0;
		}
	}

	if(!username_get(owner)) {
		strcpy(owner,"unknown");
	}

	// port = MAX_TCP_PORT + process id, this is for the catalog server to
	// distinguish the worker pools from the same host. See make_hash_key()
	// function in catalog_server.c
	INT64_T port = 65535 + pid; 

	buffer = buffer_create();
	buffer_printf(buffer, "type wq_pool\npool_name %s\nport %lld\nstarttime %llu\ndecision %s\nworkers_requested %d\nowner %s\nlifetime %d", pool_name, port, start_time, decision, workers_requested, owner, WORK_QUEUE_CATALOG_POOL_AD_LIFETIME);

	text = buffer_tostring(buffer, &text_size);
	debug(D_WQ, "Pool AD: \n%s\n", text);

	if(domain_name_cache_lookup(catalog_host, address)) {
		debug(D_WQ, "Sending the pool decision to the catalog server at %s:%d ...", catalog_host, catalog_port);
		datagram_send(outgoing_datagram, text, text_size, address, catalog_port);
	}

	buffer_delete(buffer);
	last_update_time = time(0);
	return 1;
}

void debug_print_masters(struct list *ml)
{
	struct work_queue_master *m;
	int count = 0;
	char timestr[1024];

	list_first_item(ml);
	while((m = (struct work_queue_master *) list_next_item(ml))) {
		if(timestamp_fmt(timestr, sizeof(timestr), "%R %b %d, %Y", (timestamp_t)(m->start_time)*1000000) == 0) {
			strcpy(timestr, "unknown time");
		}
		debug(D_WQ, "%d:\t%s@%s:%d started on %s\n", ++count, m->proj, m->addr, m->port, timestr);
	}
}
