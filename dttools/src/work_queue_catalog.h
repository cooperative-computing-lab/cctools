/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef WORK_QUEUE_CATALOG_H
#define WORK_QUEUE_CATALOG_H

#include "work_queue.h"
#include "work_queue_protocol.h"

#include "catalog_query.h"
#include "catalog_server.h"
#include "datagram.h"
#include "domain_name_cache.h"
#include "nvpair.h"
#include "username.h"
#include "link.h"
#include "hash_cache.h"
#include "list.h"
#include "xxmalloc.h"
#include "debug.h"
#include "stringtools.h"
#include "domain_name_cache.h"
#include "timestamp.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#define WORK_QUEUE_CATALOG_UPDATE_INTERVAL 10
#define	WORK_QUEUE_CATALOG_LIFETIME	60
#define WORK_QUEUE_NAME_MAX 256

#define WORK_QUEUE_PROTOCOL_BLANK_FIELD "-"
#define WORK_QUEUE_PROTOCOL_FIELD_MAX 256

struct work_queue_master {
	char addr[LINK_ADDRESS_MAX];
	int port;
	char proj[WORK_QUEUE_NAME_MAX];
	timestamp_t start_time;
	int priority;
	int capacity;
	int tasks_waiting;
	int tasks_running;
	int tasks_complete;
	int total_tasks_dispatched;
	int workers_init;
	int workers_ready;
	int workers_busy;
	int workers;
	char *workers_by_pool;
	char owner[USERNAME_MAX];
	int default_max_workers_from_pool;
	int workers_need;
	int workers_from_this_pool;
	int target_workers_from_pool;
};

struct work_queue_pool {
	char addr[LINK_ADDRESS_MAX];
	char name[WORK_QUEUE_POOL_NAME_MAX];
	char *decision;
	char owner[USERNAME_MAX];
};

struct pool_info {
	char name[WORK_QUEUE_POOL_NAME_MAX];
	unsigned int count;
};

void debug_print_masters(struct list *ml);

int parse_catalog_server_description(char *server_string, char **host, int *port);

struct work_queue_pool *parse_work_queue_pool_nvpair(struct nvpair *nv);

void free_work_queue_pool(struct work_queue_pool *p);

int workers_by_item(const char *workers_by_item, const char *item_name);

struct work_queue_master *parse_work_queue_master_nvpair(struct nvpair *nv);

struct work_queue_master *duplicate_work_queue_master(struct work_queue_master *master);

void free_work_queue_master(struct work_queue_master *m);

void free_work_queue_master_list(struct list *ml);

struct list *get_masters_from_catalog(const char *catalog_host, int catalog_port, struct list *regex_list);

int advertise_master_to_catalog(const char *catalog_host, int catalog_port, const char *project_name, struct work_queue_stats *s, const char *workers_summary, int now);

int get_pool_decisions_from_catalog(const char *catalog_host, int catalog_port, const char *proj, struct list *decisions);

int advertise_pool_decision_to_catalog(const char *catalog_host, int catalog_port, const char *pool_name, const char *decision);
#endif
