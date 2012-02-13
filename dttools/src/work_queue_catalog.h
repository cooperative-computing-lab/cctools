/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"

#include "catalog_query.h"
#include "catalog_server.h"
#include "datagram.h"
#include "domain_name_cache.h"
#include "nvpair.h"
#include "username.h"
#include "link.h"
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

#define WORK_QUEUE_CATALOG_LINE_MAX 1024
#define WORK_QUEUE_CATALOG_UPDATE_INTERVAL 10
#define	WORK_QUEUE_CATALOG_LIFETIME	60

struct work_queue_master {
	char addr[LINK_ADDRESS_MAX];
	int port;
	char proj[WORK_QUEUE_NAME_MAX];
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
	char owner[USERNAME_MAX];
};

void debug_print_masters(struct list *ml);

int parse_catalog_server_description(char *server_string, char **host, int *port);

struct work_queue_master *parse_work_queue_master_nvpair(struct nvpair *nv);

struct work_queue_master *duplicate_work_queue_master(struct work_queue_master *master);

struct list *get_masters_from_catalog(const char *catalog_host, int catalog_port, struct list *regex_list);

int advertise_master_to_catalog(const char *catalog_host, int catalog_port, const char *project_name, struct work_queue_stats *s);
