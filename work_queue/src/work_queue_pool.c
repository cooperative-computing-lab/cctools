/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "cctools.h"
#include "batch_job.h"
#include "hash_table.h"
#include "copy_stream.h"
#include "debug.h"
#include "envtools.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "itable.h"
#include "create_dir.h"
#include "delete_dir.h"
#include "macros.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "work_queue_catalog.h"
#include "list.h"
#include "get_line.h"
#include "getopt.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>

#define CATALOG_QUERY_INTERVAL 15
#define LOG_INTERVAL 5 

#define WORKERS_PER_JOB_MAX 50
#define EXTRA_WORKERS_MAX 20
#define EXTRA_WORKERS_PERCENTAGE 0.2

#define POOL_CONFIG_LINE_MAX 4096
#define POOL_CONFIG_MODE_ON_DEMAND 0 
#define POOL_CONFIG_MODE_FIXED 1 
#define POOL_CONFIG_USE_CAPACITY 0
#define POOL_CONFIG_IGNORE_CAPACITY 1
#define MAX_WORKERS_DEFAULT 100

#define MAX_LINE 2048

static time_t pool_start_time;
static time_t last_log_time; 
static int abort_flag = 0;
static sig_atomic_t pool_config_updated = 1;
static struct batch_queue *q;
static struct itable *job_table = NULL;
static char *decision_string = NULL;
static int make_decision_only = 0;
static int worker_timeout = 0;
static char extra_worker_args[PATH_MAX] = "";
static int retry_count = 20;
static int last_decision_amount = 0;
static time_t last_decision_time = 0;
static UINT64_T total_worker_time = 0;
static UINT64_T total_finished_cycles = 0;

static FILE *logfile = NULL;

static char name_of_this_pool[WORK_QUEUE_POOL_NAME_MAX];
typedef enum {
	TABLE_ALIGN_LEFT,
	TABLE_ALIGN_RIGHT
} table_align_t;

struct pool_table_header {
	const char *name;
	table_align_t align;
	int width;
};

static struct pool_table_header headers[] = {
	{"project", TABLE_ALIGN_LEFT, 20},
	{"host", TABLE_ALIGN_LEFT, 15},
	{"port", TABLE_ALIGN_RIGHT, 6},
	{"capacity", TABLE_ALIGN_RIGHT, 10},
	{"worker_need", TABLE_ALIGN_RIGHT, 15},
	{"worker_active", TABLE_ALIGN_RIGHT, 15},
	{"worker_assign", TABLE_ALIGN_RIGHT, 15},
	{0,0,0}
};

static void fill_string(const char *str, char *buf, int buflen, table_align_t align)
{
	int stlen = strlen(str);
	memset(buf, ' ', buflen);
	buf[buflen] = 0;
	if(align == TABLE_ALIGN_LEFT) {
		while(stlen > 0 && buflen > 0) {
			*buf++ = *str++;
			stlen--;
			buflen--;
		}
	} else {
		str = str + stlen - 1;
		buf = buf + buflen - 1;
		while(stlen > 0 && buflen > 0) {
			*buf-- = *str--;
			stlen--;
			buflen--;
		}
	}
}

void auto_pool_print_table_header(FILE * s, struct pool_table_header *h)
{
	while(h->name) {
		char *n = xxmalloc(h->width + 1);
		fill_string(h->name, n, h->width, h->align);
		string_toupper(n);
		printf("%s ", n);
		free(n);
		h++;
	}
	printf("\n");
}

void auto_pool_print_table_body(struct list *masters, FILE *stream, struct pool_table_header *header)
{
	if(!masters || !stream || !header) {
		return;
	}
	struct work_queue_master *m;	
	list_first_item(masters);
	while((m = (struct work_queue_master *)list_next_item(masters))) {
		struct pool_table_header *h = header;
		while(h->name) {
			char *aligned = xxmalloc(h->width + 1);
			char *line;
			line = xxmalloc(h->width);
			if(strcmp(h->name, "project") == 0) {
				snprintf(line, h->width, "%s", m->proj);
			} else if (strcmp(h->name, "host") == 0) {
				snprintf(line, h->width, "%s", m->addr);
			} else if (strcmp(h->name, "port") == 0) {
				snprintf(line, h->width, "%d", m->port);
			} else if (strcmp(h->name, "capacity") == 0) {
				if(m->capacity > 0) {
					snprintf(line, h->width, "%d", m->capacity);
				} else {
					strncpy(line, "unknown", h->width);
				}
			} else if (strcmp(h->name, "worker_need") == 0) {
				snprintf(line, h->width, "%d", m->workers_need);
			} else if (strcmp(h->name, "worker_active") == 0) {
				snprintf(line, h->width, "%d", m->workers_connected_from_pool);
			} else if (strcmp(h->name, "worker_assign") == 0) {
				snprintf(line, h->width, "%d", m->target_workers_from_pool);
			} else {
				strncpy(line, "???", h->width);
			}

			fill_string(line, aligned, h->width, h->align);
			printf("%s ", aligned);
			free(line);
			free(aligned);
			h++;
		}
		printf("\n");
	}
}

struct worker_status {
	int batch_job_id;
	char status;
};

struct pool_config {
	struct list *project;
	struct list *distribution;
	unsigned int max_workers;
	unsigned int min_workers;
	unsigned int default_capacity;
	unsigned int max_change_per_min;
	unsigned int billing_cycle;
	unsigned int worker_terminate_boundary;
	int capacity_mode;
	int mode;
};

static int submit_workers(const char *cmd, const char *input_files, int count);
int decide_worker_distribution(struct list *matched_masters, struct pool_config *pc, const char *catalog_host, int catalog_port);
UINT64_T get_current_billing_cycles(unsigned int);
UINT64_T get_current_worker_time();

static void handle_abort(int sig)
{
	abort_flag = 1;
}

static void handle_config(int sig)
{
	pool_config_updated = 1;
}

struct worker_distribution_node {
	char *name;
	unsigned int default_max;
	unsigned int hit;
};

void set_pool_name(char *pool_name, size_t size) {
	char hostname[DOMAIN_NAME_MAX];
	pid_t pid;

	domain_name_cache_guess(hostname);
	pid = getpid();

	snprintf(pool_name, size, "%s-%d", hostname, (int)pid);
}

void destroy_pool_config(struct pool_config **ppc) {
	struct pool_config *pc = *ppc;
	if(!pc) return;

	if(pc->project) {
		char *p;
		while((p = (char *)list_pop_tail(pc->project))) {
			free(p);
		}
	}
	list_delete(pc->project);

	if(pc->distribution) {
		struct worker_distribution_node *wdn;
		while((wdn = (struct worker_distribution_node *)list_pop_tail(pc->distribution))) {
			free(wdn->name);
			free(wdn);
		}
	}
	list_delete(pc->distribution);

	free(pc);
	pc = NULL;
}

int add_worker_distribution(struct pool_config *pc, const char *value) {
	if(!pc || !value) {
		return 0;
	}

	if(!pc->project) {
		pc->project = list_create();
		if(!pc->project) {
			fprintf(stderr, "Cannot allocate memory for list!\n");
			return 0;
		}
	}

	if(!pc->distribution) {
		pc->distribution = list_create();
		if(!pc->distribution) {
			fprintf(stderr, "Cannot allocate memory for list!\n");
			return 0;
		}
	}

	char *distribution, *d, *eq;

	distribution = strdup(value);
	d = strtok(distribution, " \t,"); 
	while(d) {
		eq = strchr(d, '=');
		if(eq) {
			int default_max;

			*eq = '\0';
			default_max = atoi(eq+1);
			if(default_max < 0) {
				*eq = '=';
				fprintf(stderr, "Default maximum number of workers in \"%s\" is invalid.\n", d);
				free(distribution);
				return 0;
			}

			struct worker_distribution_node *wdn;
			wdn = (struct worker_distribution_node *)xxmalloc(sizeof(*wdn));

			wdn->name = xxstrdup(d);
			wdn->default_max = default_max;

			list_push_tail(pc->project, xxstrdup(wdn->name));
			list_push_tail(pc->distribution, wdn);

			*eq = '=';
		} else {
			fprintf(stderr, "Invalid worker distribution item: \"%s\".\n", d);
			free(distribution);
			return 0;
		}
		d = strtok(0, " \t,");
	}
	free(distribution);

	return 1;
}

struct pool_config *pool_config_create()
{
	struct pool_config *pc = (struct pool_config *)malloc(sizeof(*pc));
	if(!pc) {
		fprintf(stderr, "Failed to allocate memory for pool configuration - %s\n", strerror(errno));
		return NULL;
	}

	pc->project = list_create();
	if(!pc->project) {
		fprintf(stderr, "Cannot allocate memory for list!\n");
		return 0;
	}

	pc->distribution = list_create();
	if(!pc->distribution) {
		fprintf(stderr, "Cannot allocate memory for list!\n");
		return 0;
	}

	// Set defaults
	pc->max_workers = MAX_WORKERS_DEFAULT;
	pc->min_workers = 0; 
	pc->default_capacity = 0;
	pc->max_change_per_min = 0;
	pc->mode = POOL_CONFIG_MODE_ON_DEMAND;
	pc->capacity_mode = POOL_CONFIG_USE_CAPACITY;
	pc->billing_cycle = 0;
	pc->worker_terminate_boundary = 0;

	return pc;
}

int read_next_pair(FILE *stream, const char *path, char *name, char *value)
{
	static int line_count = 0;
	char *line;

	while((line = get_line(stream)))
	{
		line_count++;

		string_chomp(line);
		if(string_isspace(line)) {
			continue;
		}

		if(line[0] == '#') { // skip comment lines
			continue;
		}

		char *colon = strchr(line, ':');

		if(!colon || *(colon + 1) == '\0')
		{
			fatal("Line %d in %s is invalid: \"%s\". Should be \"item_name:item_value\".\n", line_count, path, line);
		}

		*colon = '\0';
		name[MAX_LINE - 1]  = '\0';
		value[MAX_LINE - 1] = '\0';

		strncpy(name,  line,    MAX_LINE);
		strncpy(value, colon+1, MAX_LINE);

		if(name[MAX_LINE - 1] != '\0' || value[MAX_LINE - 1] != '\0') 
		{
			fatal("Line %d in %s is invalid: \"%s\". Runaway line?\n", line_count, path, line);
		}

		string_chomp(name);
		string_chomp(value);
		char *name_t  = xxstrdup(string_trim_spaces(name));
		char *value_t = xxstrdup(string_trim_spaces(value));

		strcpy(name,  name_t);
		strcpy(value, value_t);

		free(line);
		free(name_t);
		free(value_t);

		return 1;
	}

	return 0;
}


struct pool_config *get_pool_config(const char *path) {
	// Read in new configuration from file
	FILE *fp;
	fp = fopen(path, "r");
	if(!fp) {
		fprintf(stderr, "Failed to open pool configuration file: \"%s\".\n", path);
		return 0;
	}

	struct pool_config *pc = pool_config_create();

	char name[MAX_LINE];
	char value[MAX_LINE];

	while(read_next_pair(fp, path, name, value)) {
		if(!strcmp(name, "distribution")) {
			if(!add_worker_distribution(pc, value)) {
				fatal("Error loading configuration: failed to record worker distribution.\n");
			}
			continue;
		}

		if(!strcmp(name, "max_workers")) {
			pc->max_workers = atoi(value);
			if(pc->max_workers <= 0) {
				fatal("Invalid configuration: max_workers should be greater than 0 (current value: %d).\n", pc->max_workers);
			}
			continue;
		} 

		if(!strcmp(name, "min_workers")) {
			pc->min_workers = atoi(value);
			if(pc->min_workers <= 0) {
				fatal("Invalid configuration: min_workers should be greater than 0 (current value: %d).\n", pc->min_workers);
			}
			continue;
		} 

		if(!strcmp(name, "mode")) {
			if(!strcmp(value, "fixed")) {
				pc->mode = POOL_CONFIG_MODE_FIXED;
			}
			else if(!strcmp(value, "on-demand")) {
				pc->mode = POOL_CONFIG_MODE_ON_DEMAND;
			}
			else {
				fatal("Invalid configuration: mode should be one of 'fixed' or 'on-demand' (default 'on-demand', current value %s).\n", value);
			}
			continue;
		} 

		if(!strcmp(name, "ignore_capacity")) {
			if(!strcmp(value, "yes")) {
				pc->capacity_mode = POOL_CONFIG_IGNORE_CAPACITY;
			}
			if(!strcmp(value, "no")) {
				pc->capacity_mode = POOL_CONFIG_USE_CAPACITY;
			}
			else {
				fatal("Invalid configuration: ignore_capacity should be one of 'yes' or 'no' (default 'no', current value %s).\n", value);
			}
			continue;
		} 

		if(!strcmp(name, "default_capacity")) {
			pc->default_capacity = atoi(value);
			if(pc->default_capacity <= 0) {
				fatal("Invalid configuration: default_capacity, if specified, should be greater than 0 (current value: %d).\n", pc->default_capacity);
			}
			continue;
		} 

		if(!strcmp(name, "max_change_per_min")) {
			pc->max_change_per_min = atoi(value);
			if(pc->max_change_per_min <= 0) {
				fatal("Invalid configuration: max_change_per_min, if specified, should be greater than 0 (current value: %d).\n", pc->max_change_per_min);
			}
			continue;
		} 

		if(!strcmp(name, "billing_cycle")) {
			pc->billing_cycle = string_time_parse(value);
			if(pc->billing_cycle <= 0) {
				fatal("Invalid configuration: billing_cycle, if specified, should be greater than 0 (current value: %d).\n", pc->billing_cycle);
			}
			continue;
		} 

		if(!strcmp(name, "worker_terminate_boundary")) {
			pc->worker_terminate_boundary = string_time_parse(value);
			if(pc->worker_terminate_boundary <= 0) {
				fatal("Invalid configuration: worker_terminate_boundary, if specified, should be greater than 0 (current value: %d).\n", pc->worker_terminate_boundary);
			}
			continue;
		} 

		fatal("Invalid configuration: invalid item -- %s\n", name);

	} 

	fclose(fp);

	// sanity checks
	if(pc->min_workers > pc->max_workers) {
		fatal("Invalid configuration: min_workers (%d) is greater than max_workers (%d).\n", pc->min_workers, pc->max_workers);
	}

	return pc;

}

void display_pool_config(struct pool_config *pc) {
	if(!pc) return;

	printf("** Maximum Number of Workers:\n\t%d\n\n", pc->max_workers);
	if(pc->project) {
		char *p;
		int i = 0;
		printf("** Preferred Projects:\n");
		list_first_item(pc->project);
		while((p = (char *)list_next_item(pc->project))) {
			i++;
			printf("\t%d: %s\n", i, p);
		}
		printf("\n");
	}

	if(pc->distribution) {
		struct worker_distribution_node *wdn;
		int i = 0;
		printf("** Worker Distribution Assignment:\n");
		list_first_item(pc->distribution);
		while((wdn = (struct worker_distribution_node *)list_next_item(pc->distribution))) {
			i++;
			printf("\t%d: %s %d\n", i, wdn->name, wdn->default_max);
		}
		printf("\n");
	}
}

struct pool_config *update_pool_config(const char *pool_config_path, struct pool_config *old_config) {
	struct stat config_stat;
	static time_t last_modified = 0;
	struct pool_config *pc = NULL;

	if(stat(pool_config_path, &config_stat) == 0) {
		if(config_stat.st_mtime > last_modified) {
			last_modified = config_stat.st_mtime;
		} else {
			return old_config;
		}
	} else {
		fprintf(stderr, "Cannot stat pool configuration file - %s (%s)\nUsing old configuration ...\n\n", pool_config_path, strerror(errno));
		display_pool_config(old_config);
		return old_config;
	}

	// Need to load new pool configuration
	pc = get_pool_config(pool_config_path);
	if(!pc) {	
		fprintf(stderr, "New pool configuration is malformatted.\nUsing old configuration ...\n\n");
		pc = old_config;
	} else {
		destroy_pool_config(&old_config);
		printf("New work queue pool configuration has been loaded.\n\n");
	}
	display_pool_config(pc);
	return pc;
}

// This is an experimental feature!!
void start_serving_masters(const char *catalog_host, int catalog_port, const char *pool_config_path) 
{
	struct list *matched_masters;
	batch_job_id_t jobid;
	struct batch_job_info info;
	static time_t next_catalog_query_time = 0;

	char cmd[PATH_MAX] = "";
	char input_files[PATH_MAX] = "";

	struct pool_config *pc = NULL;
	while(!abort_flag) {
		if(pool_config_updated) {
			pool_config_updated = 0;
			pc = update_pool_config(pool_config_path, pc);
			if(!pc) {
				abort_flag = 1;
				fprintf(stderr, "Failed to load a valid pool configuration.\n");
				return;
			}

			if(worker_timeout > 0) {
				snprintf(cmd, PATH_MAX, "./work_queue_worker -a -B %d -C %s:%d -t %d -p %s %s", pc->worker_terminate_boundary, catalog_host, catalog_port, worker_timeout, name_of_this_pool, extra_worker_args);
			} else {
				snprintf(cmd, PATH_MAX, "./work_queue_worker -a -B %d -C %s:%d -p %s %s", pc->worker_terminate_boundary, catalog_host, catalog_port, name_of_this_pool, extra_worker_args);
			}
			snprintf(input_files, PATH_MAX, "work_queue_worker");
		}

		if(next_catalog_query_time <= time(0)) {
			next_catalog_query_time = time(0) + CATALOG_QUERY_INTERVAL;
			matched_masters = get_masters_from_catalog(catalog_host, catalog_port, pc->project);
			if(!matched_masters) {
				last_decision_time = time(0);
				last_decision_amount = 0; 
				advertise_pool_decision_to_catalog(catalog_host, catalog_port, name_of_this_pool, getpid(), pool_start_time, "n/a", itable_size(job_table));
				goto check_workers;
			}
		} else {
			goto check_workers;
		}

		debug(D_WQ, "Matching masters:\n");
		debug_print_masters(matched_masters);

		int workers_desired = decide_worker_distribution(matched_masters, pc, catalog_host, catalog_port);

		// sum_waiting is the maximum # of workers that all masters would possibly need from the pool
		// this field is useful when masters are at the end of their lifecycle (when masters have no waiting tasks)
		// without this field, the pool may submit unnecessary workers due to the stale data in catalog server
		int sum_waiting = 0;
		//  we compute the following sums for logging purposes
		int sum_masters = 0;
		int sum_running = 0;
		int sum_workers_connected = 0;
		int sum_capacity = 0;

		struct work_queue_master *m;	
		list_first_item(matched_masters);
		while((m = (struct work_queue_master *)list_next_item(matched_masters))) {
			sum_waiting += m->tasks_waiting;

			sum_masters += 1;
			// sum of the number of running tasks on each master
			sum_running += m->tasks_running;
			// m->workers_connected_from_pool only has value after decide_worker_distribution() has been called
			sum_workers_connected += m->workers_connected_from_pool; 
			// maximum possible capacity the masters have
			sum_capacity += m->capacity > 0 ? m->capacity : m->tasks_waiting; 
		}

		unsigned int workers_submitted = itable_size(job_table);

		time_t now = time(0);
		if(now - last_log_time >= LOG_INTERVAL && logfile) {	
			fprintf(logfile, "%" PRIu64 " ",  timestamp_get());
			fprintf(logfile, "%d %d ", workers_desired, workers_submitted); 
			fprintf(logfile, "%d %d %d ", sum_workers_connected, sum_masters, sum_capacity); 
			fprintf(logfile, "%d %d ",    sum_running, sum_waiting);
			fprintf(logfile, "%" PRIu64 " ", get_current_worker_time());
			fprintf(logfile, "%" PRIu64 " ", get_current_billing_cycles(pc->billing_cycle));
			fprintf(logfile, "\n");

			last_log_time = now; 

			fflush(logfile);
			fsync(fileno(logfile));
		}

		if(make_decision_only) {
			printf("Number of matched masters: %d.\n\n", list_size(matched_masters));
			if(list_size(matched_masters)) {
				auto_pool_print_table_header(stdout, headers);
				auto_pool_print_table_body(matched_masters, stdout, headers);
				printf("\n*******************************\n\n"); 
			}

			free_work_queue_master_list(matched_masters);
			matched_masters = NULL;

			advertise_pool_decision_to_catalog(catalog_host, catalog_port, name_of_this_pool, getpid(), pool_start_time, decision_string, 0);

			sleep(5);
			continue;
		}

		int n = workers_desired - itable_size(job_table);
		if(n > 0) {
			// The following line of code is important. At the end of workloads
			// (i.e., no waiting tasks), the decision would allow each master
			// to hold whatever number of workers that have already had (so
			// those workers won't discconnect and bounce between masters), and
			// some of those would time out -- making itable_size(job_table) <
			// workers_desired (the decision). But since no enough waiting
			// tasks left, we don't need to resubmit those timed out workers.
			n = MIN(n, sum_waiting);

			if(n > 0) {
				submit_workers(cmd, input_files, n);
				printf("%d more workers has just been submitted.\n", n);
			}
		} 

		workers_submitted = itable_size(job_table);
		printf("%d workers are being maintained.\n", workers_submitted);

		advertise_pool_decision_to_catalog(catalog_host, catalog_port, name_of_this_pool, getpid(), pool_start_time, decision_string, MAX(workers_submitted, pc->min_workers));

		printf("Number of matched masters: %d.\n\n", list_size(matched_masters));
		if(list_size(matched_masters)) {
			auto_pool_print_table_header(stdout, headers);
			auto_pool_print_table_body(matched_masters, stdout, headers);
			printf("\n*******************************\n\n"); 
		}

		free_work_queue_master_list(matched_masters);
		matched_masters = NULL;

check_workers:
		if(pc->min_workers > 0) {
			int extra_workers_needed = pc->min_workers - itable_size(job_table);
			if(extra_workers_needed > 0) {
				submit_workers(cmd, input_files, extra_workers_needed);
				printf("%d more workers has just been submitted to meet the min_workers requirement.\n", extra_workers_needed);
			}
		}

		if(itable_size(job_table)) {
			jobid = batch_job_wait_timeout(q, &info, time(0) + 5);
			if(jobid >= 0 && !abort_flag) {
				struct batch_job_info *submit_info = itable_lookup(job_table, jobid);
				if(submit_info) {
					time_t job_time;
					if(info.started > 0) {
						job_time = info.finished - info.started;
					} else {
						job_time = info.finished - submit_info->submitted;
					}
					total_worker_time += job_time; 

					if(pc->billing_cycle > 0) {
						UINT64_T finished_cycles = job_time / pc->billing_cycle;
						if(job_time % pc->billing_cycle != 0) {
							finished_cycles += 1;
						}
						total_finished_cycles += finished_cycles;
					}

					free(submit_info);
				}
				itable_remove(job_table, jobid);
			}
		} else {
			sleep(5);
		}
	}

	// aborted
	free_work_queue_master_list(matched_masters);
	if(pc) {
		destroy_pool_config(&pc);
	}
	if(decision_string) {
		free(decision_string);
	}
}

char *get_pool_decision_string(struct list *ml) {
	struct work_queue_master *m;
	char *string;
	const int increment = 1024;
	int length = increment;

	if(!(string = (char *)malloc(length * sizeof(char)))) {
		fprintf(stderr, "Writing 'workers by pool' information to string failed: %s", strerror(errno));
		return NULL;
	}


	int i = 0;
	list_first_item(ml);
	while((m = (struct work_queue_master *)list_next_item(ml))) {
		int n, size;

retry:
		size = length - i;
		n = snprintf(string + i, size, "%s:%d,", m->proj, m->target_workers_from_pool);

		if(n <= 0) {
			fprintf(stderr, "failed to advertise decision item: %s:%d\n", m->proj, m->target_workers_from_pool);
			continue;
		}

		if(n >= size) {
			length += increment;

			char *old_string = string;
			printf("realloc: %d", length); 
			if(!(string = (char *)realloc(string, length * sizeof(char)))) {
				fprintf(stderr, "Realloc memory for 'decision' string failed: %s", strerror(errno));
				free(old_string);
				return NULL;
			}
			goto retry;
		}
		i += n;
	}

	if(i > 0) {
		*(string + i -1) = '\0';
	} else {
		strncpy(string, "n/a", 4);
	}

	return string;
}

UINT64_T get_current_worker_time() {
	struct batch_job_info *info;
	UINT64_T sum_running_worker_time = 0;
	UINT64_T key;
	
	itable_firstkey(job_table);
	while(itable_nextkey(job_table, &key, (void **)(&info))) {
		sum_running_worker_time += time(0) - info->submitted; 
	}

	return sum_running_worker_time + total_worker_time;
}

UINT64_T get_current_billing_cycles(unsigned int billing_cycle) {
	struct batch_job_info *info;
	UINT64_T sum_running_cycles = 0;
	UINT64_T key;
	
	if(billing_cycle <= 0) {
		return 0;
	}
	itable_firstkey(job_table);
	while(itable_nextkey(job_table, &key, (void **)(&info))) {
		time_t elapsed = time(0) - info->submitted;

		int current_cycles = elapsed / billing_cycle;
		if(elapsed % billing_cycle != 0) {
			current_cycles += 1;
		}
		sum_running_cycles += current_cycles; 
	}

	return sum_running_cycles + total_finished_cycles;
}

int decide_worker_distribution(struct list *matched_masters, struct pool_config *pc, const char *catalog_host, int catalog_port) {
	struct work_queue_master *m;

	if(!pc || !matched_masters) {
		return -1;
	}

	void **pointers;
	pointers = (void **)xxmalloc(list_size(matched_masters) * sizeof(void *));

	// set default max workers from this pool for every master
	int sum = 0;
	struct worker_distribution_node *wdn;
	list_first_item(pc->distribution);
	while((wdn = (struct worker_distribution_node *)list_next_item(pc->distribution))) {
		wdn->hit = 0;	// how many projects are matched to a distribution node

		int i = 0;
		list_first_item(matched_masters);
		while((m = (struct work_queue_master *)list_next_item(matched_masters))) {
			if(whole_string_match_regex(m->proj, wdn->name)) {
				wdn->hit++;
				pointers[i] = m;
				i++;
			}
		}

		if( i == 0) {
			continue;
		}

		int default_max = wdn->default_max / wdn->hit;

		int j;
		for(j = 0; j < i; j++) {
			m = pointers[j];
			m->default_max_workers_from_pool = default_max;
			sum += default_max;
		}
	}

	int max_workers;
	if(pc->max_change_per_min > 0) {
		double max_change = ((double)(time(0) - last_decision_time)/60) * pc->max_change_per_min + 0.5;
		max_workers = MIN(pc->max_workers, last_decision_amount + max_change); 
	} else {
		max_workers = pc->max_workers;
	}

	// shrink default_max if needed
	if(sum > max_workers) {
		double shrink_factor;
		struct work_queue_master *tmp = NULL;
		shrink_factor = (double) max_workers/ sum;
		sum = 0;
		list_first_item(matched_masters);
		while((m = (struct work_queue_master *)list_next_item(matched_masters))) {
			//printf("proj: %s; target: %d; ",m->proj, m->target_workers_from_pool);
			m->default_max_workers_from_pool = (double)m->default_max_workers_from_pool * shrink_factor + 0.5;
			//printf("after shrink: %d\n", m->target_workers_from_pool);
			sum += m->default_max_workers_from_pool;
			tmp = m;
		}
		if(tmp) {
			tmp->default_max_workers_from_pool += max_workers - sum;
		}
		sum = max_workers;
	}

	int sum_decided_workers = 0; 

	if(pc->mode == POOL_CONFIG_MODE_FIXED) { // pc->mode == POOL_CONFIG_MODE_FIXED
		list_first_item(matched_masters);
		while((m = (struct work_queue_master *)list_next_item(matched_masters))) {
			m->target_workers_from_pool = m->default_max_workers_from_pool; // final decision
		}
		sum_decided_workers = max_workers;
	} else { // pc->mode == POOL_CONFIG_MODE_ON_DEMAND
		// make final decisions on masters whose needs are less than its default max allowed.
		list_first_item(matched_masters);
		int i = 0;
		while((m = (struct work_queue_master *) list_next_item(matched_masters))) {
			if(m->capacity > 0 && pc->capacity_mode == POOL_CONFIG_USE_CAPACITY) {
				m->workers_need = m->capacity - m->workers;
				if(m->workers_need < 0) {
					m->workers_need = 0;
				}
				m->workers_need = MIN(m->workers_need, m->tasks_waiting);
			} else {
				if(pc->default_capacity > 0) {
					m->workers_need = MIN( (int) (pc->default_capacity - m->workers), m->tasks_waiting); 
					if(m->workers_need < 0) {
						m->workers_need = 0;
					}
				} else {
					m->workers_need = m->tasks_waiting;
				}
			}

			m->workers_connected_from_pool = workers_by_item(m->workers_by_pool, name_of_this_pool); 
			if(m->workers_connected_from_pool == -1) {
				m->workers_connected_from_pool = 0;
			}

			m->workers_need_from_pool = m->workers_need + m->workers_connected_from_pool;

			if(m->default_max_workers_from_pool >= m->workers_need_from_pool) {
				// This is final decision
				m->target_workers_from_pool = m->workers_need_from_pool;
				sum_decided_workers += m->target_workers_from_pool; 
			} else {
				m->target_workers_from_pool = m->default_max_workers_from_pool;
				pointers[i] = m;
				i++;
			}
		}

		int sum_need_of_hungry_masters, sum_weight_of_hungry_masters;
		int hungry_masters = i;
		int workers_undecided = max_workers - sum_decided_workers;
		while(1) {
			sum_need_of_hungry_masters = 0;
			sum_weight_of_hungry_masters = 0;
			int j;
			for(j = 0; j < hungry_masters; j++) {
				m = pointers[j];
				sum_weight_of_hungry_masters += m->default_max_workers_from_pool;
				sum_need_of_hungry_masters += m->workers_need_from_pool;
			}

			if(sum_need_of_hungry_masters <= workers_undecided) { 
				// all remaining hungry masters can be provided with workers up to its workers_need_from_pool 
				for(j = 0; j < hungry_masters; j++) {
					m = pointers[j];
					m->target_workers_from_pool = m->workers_need_from_pool; 
					sum_decided_workers += m->target_workers_from_pool; 
				}

				break;
			}

			i = 0; // number of finalized hungry masters in this iteration
			for(j = 0; j < hungry_masters; j++) {
				m = pointers[j];

				double portion = (double)m->default_max_workers_from_pool / sum_weight_of_hungry_masters;
				m->target_workers_from_pool = (double)workers_undecided * portion + 0.5; // potential decision
				
				if(m->target_workers_from_pool >= m->workers_need_from_pool) {
					m->target_workers_from_pool = m->workers_need_from_pool; // final decision
					sum_decided_workers += m->target_workers_from_pool; 

					pointers[j] = pointers[hungry_masters-1];
					pointers[hungry_masters-1] = NULL;

					hungry_masters--;
					
					i++;
				}
			}

			workers_undecided = max_workers - sum_decided_workers;

			if (i == 0) {
				for(j = 0; j < hungry_masters - 1; j++) {
					m = pointers[j];
					sum_decided_workers += m->target_workers_from_pool; 
				}
				// last finalized hungry master (fixing potential off-by-one error that could introduced by fraction calculations)
				m = pointers[hungry_masters-1];
				m->target_workers_from_pool = max_workers - sum_decided_workers; 

				sum_decided_workers = max_workers;
				break;
			}
		}
	}

	free(pointers);

	last_decision_time = time(0);
	last_decision_amount = sum_decided_workers;

	// advertise decision to the catalog server
	if(decision_string) {
		free(decision_string);
		decision_string = NULL;
	}
	if((decision_string = get_pool_decision_string(matched_masters)) == NULL) {
		fprintf(stderr, "Failed to convert pool decisions into a single string.\n");
	}

	return sum_decided_workers; 
}

static int submit_workers(const char *cmd, const char *input_files, int count)
{
	int i;
	batch_job_id_t jobid;

	for(i = 0; i < count; i++) {
		debug(D_DEBUG, "Submitting job %d: %s\n", i + 1, cmd);
		jobid = batch_job_submit_simple(q, cmd, input_files, NULL);
		if(jobid >= 0) {
			struct batch_job_info *info = (struct batch_job_info *)malloc(sizeof(struct batch_job_info));
			memset(info, 0, sizeof(struct batch_job_info));
			info->submitted = time(0); 

			itable_insert(job_table, jobid, info);
		} else {
			retry_count--;
			if(!retry_count) {
				fprintf(stderr, "Retry max reached. Stop submitting more workers..\n");
				break;
			}
			fprintf(stderr, "Failed to submit the %dth job: %s. Will retry it.\n", i + 1, cmd);
			i--;
		}
	}
	return i;
}

static void remove_workers(struct itable *jobs)
{
	void *x;
	UINT64_T key;

	itable_firstkey(jobs);
	while(itable_nextkey(jobs, &key, &x)) {
		// The key is the job id
		printf("work_queue_pool: aborting remote job %" PRIu64 "\n", key);
		batch_job_remove(q, key);
		struct batch_job_info *info = itable_lookup(job_table, key);
		if(info) {
			free(info);
		}
		itable_remove(job_table, key);
	}
}

static void check_jobs_status_condor(struct itable **running_jobs, struct itable **idle_jobs, struct itable **bad_jobs)
{
	FILE *fp;
	char line[128];
	/** Sample condor_q formatting. Source: https://condor-wiki.cs.wisc.edu/index.cgi/wiki?p=HowToWriteaCondorqWrapper
	char *cmd = "condor_q \
				-format '%4d.' ClusterId \
				-format '%-3d ' ProcId \
				-format '%-14s ' Owner \
				-format '%-11s ' 'formatTime(QDate,\"%m/%d %H:%M\")' \
				-format '%3d+' 'int(RemoteUserCpu/(60*60*24))' \
				-format '%02d:' 'int((RemoteUserCpu-(int(RemoteUserCpu/(60*60*24))*60*60*24))/(60*60))' \
				-format '%02d:' 'int((RemoteUserCpu-(int(RemoteUserCpu/(60*60))*60*60))/(60))' \
				-format '%02d ' 'int(RemoteUserCpu-(int(RemoteUserCpu/60)*60))' \
				-format '%-2s ' 'ifThenElse(JobStatus==0,\"U\",ifThenElse(JobStatus==1,\"I\",ifThenElse(JobStatus==2,\"R\",ifThenElse(JobStatus==3,\"X\",ifThenElse(JobStatus==4,\"C\",ifThenElse(JobStatus==5,\"H\",ifThenElse(JobStatus==6,\"E\",string(JobStatus))))))))' \
				-format '\%-3d ' JobPrio \
				-format '\%-4.1f ' ImageSize/1024.0 \
				-format '\%-18.18s' 'strcat(Cmd,\" \",Arguments)' \
				-format '\n' Owner";
	*/

	// We don't need the ProcId because ClusterId is the unique job id for each condor task in our case.
	char *cmd = "condor_q \
				-format '%4d\t' ClusterId \
				-format '%-2s ' 'ifThenElse(JobStatus==0,\"U\",ifThenElse(JobStatus==1,\"I\",ifThenElse(JobStatus==2,\"R\",ifThenElse(JobStatus==3,\"X\",ifThenElse(JobStatus==4,\"C\",ifThenElse(JobStatus==5,\"H\",ifThenElse(JobStatus==6,\"E\",string(JobStatus))))))))' \
				-format '\n' Owner";

	fp = popen(cmd, "r");

	// Parse condor_q result
	int jobid;
	char status;
	char hash_key[128];
	struct worker_status *ws;
	struct hash_table *all_job_status;

	all_job_status = hash_table_create(0, 0);
	while(fgets(line, sizeof(line), fp)) {
		if(sscanf(line, "%d\t%c\n", &jobid, &status) == 2) {
			ws = (struct worker_status *) malloc(sizeof(struct worker_status));
			if(!ws) {
				fprintf(stderr, "Cannot record status for job %d (%s)", jobid, strerror(errno));
			}
			ws->batch_job_id = jobid;
			ws->status = status;

			sprintf(hash_key, "%d", jobid);
			hash_table_insert(all_job_status, hash_key, ws);
		} else {
			fprintf(stderr, "Unrecognized line in condor_q output: %s", line);
		}
	}
	pclose(fp);

	// Insert jobs to their corresponding job tables
	void *x;
	UINT64_T ikey;
	struct itable *running_job_table;
	struct itable *idle_job_table;
	struct itable *bad_job_table;

	running_job_table = itable_create(0);
	idle_job_table = itable_create(0);
	bad_job_table = itable_create(0);

	itable_firstkey(job_table);
	while(itable_nextkey(job_table, &ikey, &x)) {
		sprintf(hash_key, "%d", (int) ikey);
		ws = hash_table_lookup(all_job_status, hash_key);
		if(ws) {
			//printf("%d\t%c\n", ws->batch_job_id, ws->status);
			if(ws->status == 'R') {
				itable_insert(running_job_table, ikey, NULL);
			} else if(ws->status == 'I') {
				itable_insert(idle_job_table, ikey, NULL);
			} else {
				itable_insert(bad_job_table, ikey, NULL);
			}
		}
	}

	*running_jobs = running_job_table;
	*idle_jobs = idle_job_table;
	*bad_jobs = bad_job_table;

	// Remove hash table
	char *key;
	hash_table_firstkey(all_job_status);
	while(hash_table_nextkey(all_job_status, &key, (void **) &ws)) {
		free(ws);
	}
	hash_table_delete(all_job_status);
}

static int guarantee_x_running_workers_local(const char *cmd, const char *input_files, int goal)
{
	int goal_achieved = 0;
	int count;

	count = submit_workers(cmd, input_files, goal);
	if(count == goal)
		goal_achieved = 1;

	return goal_achieved;
}

static int guarantee_x_running_workers_condor(const char *cmd, const char *input_files, int goal)
{
	struct itable *running_jobs;
	struct itable *idle_jobs;
	struct itable *bad_jobs;
	void *x;
	UINT64_T key;

	int submitted = 0;
	int running, idle, bad;
	int goal_achieved = 0;
	int count, extra;

	extra = MIN(EXTRA_WORKERS_MAX, goal * EXTRA_WORKERS_PERCENTAGE);
	count = submit_workers(cmd, input_files, goal + extra);
	submitted += count;
	printf("Target number of running workers is %d and %d workers has been submitted successfully.\n", goal, count);

	while(!abort_flag) {
		check_jobs_status_condor(&running_jobs, &idle_jobs, &bad_jobs);
		running = itable_size(running_jobs);
		idle = itable_size(idle_jobs);
		bad = itable_size(bad_jobs);
		if(submitted > running + idle + bad) {
			sleep(3);
			continue;
		}
		printf("Running Jobs: %d, Idle Jobs: %d, Bad Jobs: %d\n", running, idle, bad);

		count = running - goal;
		if(count >= 0) {
			// Remove excessive jobs
			struct itable *excessive_running_jobs = itable_create(0);
			itable_firstkey(running_jobs);
			while(itable_nextkey(running_jobs, &key, &x) && count) {
				// The key is the job id
				itable_insert(excessive_running_jobs, key, x);
				count--;
			}

			remove_workers(excessive_running_jobs);
			remove_workers(idle_jobs);
			remove_workers(bad_jobs);

			itable_delete(excessive_running_jobs);
			goal_achieved = 1;
			break;
		}

		count = goal + extra - running - idle;
		if(count > 0) {
			// submit more
			count = submit_workers(cmd, input_files, count);
			submitted += count;
			printf("%d more workers has been submitted successfully.\n", count);
		}
		// We can delete these hash tables directly because the data (e->value) of each entry is NULL
		itable_delete(running_jobs);
		itable_delete(idle_jobs);
		itable_delete(bad_jobs);

		sleep(3);
	}

	if(abort_flag) {
		check_jobs_status_condor(&running_jobs, &idle_jobs, &bad_jobs);
		remove_workers(running_jobs);
		remove_workers(idle_jobs);
		remove_workers(bad_jobs);
		debug(D_WQ, "All jobs aborted.\n");
	}

	return goal_achieved;
}

static void start_x_running_workers(int batch_queue_type, const char *worker_cmd, const char *worker_input_files, int goal) {
	if(batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
		guarantee_x_running_workers_condor(worker_cmd, worker_input_files, goal);
	} else if(batch_queue_type == BATCH_QUEUE_TYPE_LOCAL) {
		guarantee_x_running_workers_local(worker_cmd, worker_input_files, goal);
	} else {
		fprintf(stderr, "Sorry! Batch queue type \"%s\" is not supported for \"-q\" option at this time.\n", batch_queue_type_to_string(batch_queue_type));
		fprintf(stderr, "Currently supported batch queue type(s) for \"-q\": \n");
		fprintf(stderr, "%s\n", batch_queue_type_to_string(BATCH_QUEUE_TYPE_CONDOR));
		fprintf(stderr, "%s\n", batch_queue_type_to_string(BATCH_QUEUE_TYPE_LOCAL));
		fprintf(stderr, "\n");
	}
} 


static void maintain_x_submitted_workers(const char *worker_cmd, const char *worker_input_files, int goal) {
	batch_job_id_t jobid;
	struct batch_job_info info;
	int count = submit_workers(worker_cmd, worker_input_files, goal);
	printf("%d workers are submitted successfully.\n", count);
	while(!abort_flag) {
		jobid = batch_job_wait_timeout(q, &info, time(0) + 5);
		if(jobid >= 0 && !abort_flag) {
			struct batch_job_info *info = itable_lookup(job_table, jobid);
			if(info) {
				free(info);
			}
			itable_remove(job_table, jobid);
			jobid = batch_job_submit_simple(q, worker_cmd, worker_input_files, NULL);
			if(jobid >= 0) {
				itable_insert(job_table, jobid, NULL);
			}
		}
	}
}


static int locate_executable(char *name, char *path)
{
	if(strlen(path) > 0) {
		if(access(path, R_OK | X_OK) < 0) {
			fprintf(stderr, "Inaccessible %s specified: %s\n", name, path);
			return 0;
		}
	} else {
		if(!find_executable(name, "PATH", path, PATH_MAX)) {
			fprintf(stderr, "Please add %s to your PATH or specify it explicitly.\n", name);
			return 0;
		}
	}
	debug(D_DEBUG, "%s path: %s", name, path);
	return 1;
}

static int copy_executable(char *current_path, char *new_path)
{
	FILE *ifs, *ofs;

	ifs = fopen(current_path, "r");
	if(ifs == NULL) {
		fprintf(stderr, "Unable to open %s for reading: %s\n", current_path, strerror(errno));
		return 0;
	}
	ofs = fopen(new_path, "w+");
	if(ofs == NULL) {
		fprintf(stderr, "Unable to open %s for writing: %s", new_path, strerror(errno));
		fclose(ifs);
		return 0;
	}
	copy_stream_to_stream(ifs, ofs);
	fclose(ifs);
	fclose(ofs);
	chmod(new_path, 0777);

	return 1;
}

static void show_help(const char *cmd)
{
    /* Hack to remove wq from the list of available batch systems. Assumes wq
     * is not the last in the list.*/
    char *queue_types = xxstrdup(batch_queue_type_string());
    char *wq          = strstr(queue_types, "wq, ");
    if(wq)
    {
        *wq  = '\0';
         wq += 4;
    }

    fprintf(stdout, "Use: %s [options] <count>\n", cmd);
    fprintf(stdout, "where batch options are:\n");
    fprintf(stdout, " %-30s Enable debugging for this subsystem.\n", "-d,--debug=<subsystem>");
    fprintf(stdout, " %-30s Log work_queue_pool status to a file whose path is <path>.\n", "-l,--logfile=<path>");
    fprintf(stdout, " %-30s Scratch directory. (default is /tmp/${USER}-workers)\n", "-S,--scratch=<path>");

    fprintf(stdout, " %-30s Batch system type. One of: (default is local)\n", "-T,--batch-type=<type>");
    if(wq)
	    fprintf(stdout, " %-30s %s%s\n", "", queue_types, wq);
    else
	    fprintf(stdout, " %-30s %s\n", "", queue_types);

    fprintf(stdout, " %-30s Number of attemps to retry if failed to submit a worker.\n", "-r,--retry=<count>");
    fprintf(stdout, " %-30s Each batch job will start <count> local workers. (default is 1.)\n", "-m,--workers-per-job=<count>");
    fprintf(stdout, " %-30s Path to the work_queue_worker executable.\n", "-W,--worker-executable=<path>");
    fprintf(stdout, " %-30s Enable auto worker pool feature.\n", "-A,--auto-pool-feature");
    fprintf(stdout, " %-30s Path to the pool configuration file. This option is only effective\n", "-c,--config=<path>");
    fprintf(stdout, " %-30s when '-A' is present. (default is work_queue_pool.conf)\n", "");
    fprintf(stdout, " %-30s Guarantee <count> running workers and quit. The workers would terminate\n", "-q,--one-shot");
    fprintf(stdout, " %-30s after their idle timeouts unless the user explicitly shut them down.\n", "");
    fprintf(stdout, " %-30s The user needs to manually delete the scratch directory, which is.\n", "");
    fprintf(stdout, " %-30s displayed on screen right before work_queue_pool exits.\n", "");
    fprintf(stdout, " %-30s Show this screen.\n", "-h,--help");

    fprintf(stdout, "\nwhere worker options are:\n");

    fprintf(stdout, " %-30s Enable auto mode. In this mode the workers would ask a catalog\n", "-a,--advertise");
    fprintf(stdout, " %-30s server for available masters. (depreacate, implied by -M).\n", "");
    fprintf(stdout, " %-30s Abort after this amount of idle time.\n", "-t,--timeout=<time>");
    fprintf(stdout, " %-30s Set catalog server to <catalog>. Format: HOSTNAME:PORT \n", "-C,--catalog=<catalog>");
    fprintf(stdout, " %-30s Name of a preferred project. A worker can have multiple preferred\n", "-M,--master-name=<project>");
    fprintf(stdout, " %-30s projects.\n", "");
    fprintf(stdout, " %-30s Same as -M,--master-name (deprecated).\n", "-N");
    fprintf(stdout, " %-30s Send worker debugging output to this file.\n", "-o,--debug-file=<file>");
    fprintf(stdout, " %-30s Extra options that should be added to the worker.\n", "-E,--extra-options=<options>");
}

int main(int argc, char *argv[])
{
	int count;
    signed char c;
	FILE *fp;
	int goal = 0;
	char scratch_dir[PATH_MAX] = "";

	char worker_cmd[PATH_MAX] = "";
	char worker_path[PATH_MAX] = "";
	char worker_args[PATH_MAX] = "";
	char worker_input_files[PATH_MAX] = "";
	char pool_path[PATH_MAX] = "";
	char new_pool_path[PATH_MAX] = "";
	char new_worker_path[PATH_MAX];
	char starting_dir_canonical_path[PATH_MAX];
	char *pool_config_path = "work_queue_pool.conf";
	char pool_config_canonical_path[PATH_MAX];
	char pool_pid_canonical_path[PATH_MAX];
	char pool_log_canonical_path[PATH_MAX];
	char pool_name_canonical_path[PATH_MAX];
	char pidfile_path[PATH_MAX];
	char poolnamefile_path[PATH_MAX];

	pool_pid_canonical_path[0] = 0;
	pool_name_canonical_path[0] = 0;

	int batch_queue_type = BATCH_QUEUE_TYPE_LOCAL;

	int auto_worker = 0;
	int guarantee_x_running_workers_and_quit = 0;
	int auto_worker_pool = 0;
	int workers_per_job = 0;

	char *catalog_host;
	int catalog_port;


	struct list *regex_list;

	regex_list = list_create();
	if(!regex_list) {
		fprintf(stderr, "cannot allocate memory for regex list!\n");
		exit(1);
	}

	catalog_host = CATALOG_HOST;
	catalog_port = CATALOG_PORT;

	set_pool_name(name_of_this_pool, WORK_QUEUE_POOL_NAME_MAX);

	getcwd(starting_dir_canonical_path,PATH_MAX);
	char *p;
	int len = strlen(starting_dir_canonical_path);
	if(len > 0) {
		p = &(starting_dir_canonical_path[len - 1]);
		if(*p != '/' && len + 1 < PATH_MAX) {
			*(p+1) = '/';
			*(p+2) = '\0';
		}
	} else {
		fprintf(stderr, "cannot get the absolute path of the current directory!\n");
		return EXIT_FAILURE;
	}

	pool_start_time = time(0);
	last_log_time = time(0) - LOG_INTERVAL;
	last_decision_time = time(0);

	debug_config(argv[0]);

	static struct option long_options[] = {
		{"debug", required_argument, 0, 'd'},
		{"logfile", required_argument, 0, 'l'},
		{"scratch", required_argument, 0, 'S'},
		{"batch-type", required_argument, 0, 'T'},
		{"retry", required_argument, 0, 'r'},
		{"workers-per-job", required_argument, 0, 'm'},
		{"worker-executable", required_argument, 0, 'W'},
		{"auto-pool-feature", no_argument,       0, 'A'},
		{"config", required_argument, 0, 'c'},
		{"advertise", no_argument,       0, 'a'},
		{"timeout", required_argument, 0, 't'},
		{"catalog", required_argument, 0, 'C'},
		{"master-name", required_argument, 0, 'M'},
		{"debug-file", required_argument, 0, 'o'},
		{"extra-options", required_argument, 0, 'E'},
		{"version", no_argument, 0, 'v'},
		{"help", no_argument, 0, 'h'},
		{0,0,0,0}
	};

	while((c = getopt_long(argc, argv, "aAc:C:d:E:hm:l:L:M:N:o:O:Pqr:S:t:T:vW:", long_options, NULL)) > -1) {
		switch (c) {
		case 'a':
			//Backwards compatibility, -a is not needed anymore.
			auto_worker = 1;
			break;
		case 'C':
			if(!parse_catalog_server_description(optarg, &catalog_host, &catalog_port)) {
				fprintf(stderr, "The provided catalog server is invalid. The format of the '-C' option is '-C HOSTNAME:PORT'.\n");
				exit(1);
			}

			if(catalog_port < 0 || catalog_port > 65535) {
				fprintf(stderr, "The provided catalog server port - '%d' is out of range.\n", catalog_port);
				exit(1);
			}

			strcat(worker_args, " -C ");
			strcat(worker_args, optarg);
			break;
		case 'E':
			// for use in start_serving_masters function
			strcat(extra_worker_args, optarg); 
			break;
		case 'N':
		case 'M':
			auto_worker = 1;
			strcat(worker_args, " -M ");
			strcat(worker_args, optarg);
			list_push_tail(regex_list, strdup(optarg));
			break;
		case 't':
			strcat(worker_args, " -t ");
			strcat(worker_args, optarg);
			worker_timeout = atoi(optarg);
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'l':
			if(optarg[0] == '/') {
				strncpy(pool_log_canonical_path, optarg, PATH_MAX);
			} else {
				char *p = optarg;
				if(strlen(optarg) > 2) {
					if(!strncmp(pool_config_path, "./", 2)) {
						p = pool_config_path + 2;
					}
				}
				strncpy(pool_log_canonical_path, starting_dir_canonical_path, PATH_MAX);
				strncat(pool_log_canonical_path, p, strlen(p));
			}

			if((logfile = fopen(pool_log_canonical_path, "a")) != NULL) {
				printf("Log is written to file '%s'.\n", pool_log_canonical_path);
			} else {
				fprintf(stderr, "Failed to open log file '%s' for appending. No log would be generated for this run.\n", pool_log_canonical_path);
			}
			break;
		case 'm':
			count = atoi(optarg);
			if(count < 0 || count > WORKERS_PER_JOB_MAX) {
				workers_per_job = 0;
			} else {
				workers_per_job = count;
			}
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'O':
			debug_config_file_size(string_metric_parse(optarg));
			break;
		case 'P':
			auto_worker_pool = 1;
			make_decision_only = 1;
			break;
		case 'q':
			guarantee_x_running_workers_and_quit = 1;
			break;
		case 'A':
			auto_worker_pool = 1;
			break;
		case 'c':
			auto_worker_pool = 1;
			pool_config_path = xxstrdup(optarg);
			break;
		case 'T':
			batch_queue_type = batch_queue_type_from_string(optarg);
			if(batch_queue_type == BATCH_QUEUE_TYPE_UNKNOWN) {
				fprintf(stderr, "unknown batch queue type: %s\n", optarg);
				return EXIT_FAILURE;
			} else if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
				fprintf(stderr, "Invalid batch queue type: %s\n", optarg);
				return EXIT_FAILURE;
			}
			break;
		case 'W':
			strncpy(worker_path, optarg, PATH_MAX);
			break;
		case 'S':
			strncpy(scratch_dir, optarg, PATH_MAX);
			break;
		case 'r':
			retry_count = atoi(optarg);
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			exit(EXIT_SUCCESS);
		case 'h':
			show_help(argv[0]);
			exit(EXIT_SUCCESS);
		default:
			show_help(argv[0]);
			return EXIT_FAILURE;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if(!auto_worker_pool) {
		if(!auto_worker) {
			if((argc - optind) != 3) {
				fprintf(stderr, "invalid number of arguments\n");
				show_help(argv[0]);
				return EXIT_FAILURE;
			}
			// Add host name
			strcat(worker_args, " ");
			strcat(worker_args, argv[optind]);
			// Add port
			strcat(worker_args, " ");
			strcat(worker_args, argv[optind + 1]);
			// Number of workers to submit
			goal = strtol(argv[optind + 2], NULL, 10);
		} else {
			if((argc - optind) != 1) {
				fprintf(stderr, "invalid number of arguments\n");
				show_help(argv[0]);
				return EXIT_FAILURE;
			}
			goal = strtol(argv[optind], NULL, 10);
		}
	} else {
		pid_t pid;
		pid = getpid();
		char *p;
		p = strrchr(argv[0], '/');
		if(!p) {
			p = argv[0];
		} else {
			p++;
		}

		struct stat tmp_stat;

		if(make_decision_only) {
			// Write pool name to file
			snprintf(poolnamefile_path, PATH_MAX, "%s.poolname", p);

			if(stat(poolnamefile_path, &tmp_stat) == 0) {
				fprintf(stderr, "Error: file '%s' already exists but %s is trying to store the pool name of itself in this file.\n", poolnamefile_path, argv[0]);
				return EXIT_FAILURE;
			}

			FILE *fp = fopen(poolnamefile_path, "w");
			if(!fp) {
				fprintf(stderr, "Error: can't create file - '%s' for storing poolname: %s\n", poolnamefile_path, strerror(errno)); 
				return EXIT_FAILURE;
			}

			if(fprintf(fp, "%s\n", name_of_this_pool) < 0) {
				fprintf(stderr, "Error: failed to write pool name to file - '%s'.\n", poolnamefile_path); 
				fclose(fp);
				unlink(poolnamefile_path);
				return EXIT_FAILURE;
			}	
			fclose(fp);
		}

		// Write pid to file
		snprintf(pidfile_path, PATH_MAX, "%s.pid", p);

		if(stat(pidfile_path, &tmp_stat) == 0) {
			fprintf(stderr, "Error: file '%s' already exists but %s is trying to store the pid of itself in this file.\n", pidfile_path, argv[0]);
			return EXIT_FAILURE;
		}

		fp = fopen(pidfile_path, "w");
		if(!fp) {
			fprintf(stderr, "Error: can't create file - '%s' for storing pid: %s\n", pidfile_path, strerror(errno)); 
			return EXIT_FAILURE;
		}

		if(fprintf(fp, "%d\n",(int)pid) < 0) {
			fprintf(stderr, "Error: failed to write pid to file - '%s'.\n", pidfile_path); 
			fclose(fp);
			unlink(pidfile_path);
			return EXIT_FAILURE;
		}	
		fclose(fp);
	}

	signal(SIGINT, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGTERM, handle_abort);

	signal(SIGUSR1, handle_config);


	if(!locate_executable("work_queue_worker", worker_path))
		return EXIT_FAILURE;

	if(workers_per_job) {
		if(batch_queue_type == BATCH_QUEUE_TYPE_LOCAL) {
			fprintf(stderr, "Error: '-m' option is not intended for the \"local\" batch queue type (which is the default if the '-T' option is not specified).\n");
			return EXIT_FAILURE;
		}
		if(!locate_executable("work_queue_pool", pool_path))
			return EXIT_FAILURE;
	}
	// Create a tmp directory to hold all workers' runtime information
	if(strlen(scratch_dir) <= 0) {
		if(batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
			snprintf(scratch_dir, PATH_MAX, "/tmp/%s-workers/%ld", getenv("USER"), (long) time(0));
		} else {
			snprintf(scratch_dir, PATH_MAX, "%s-workers/%ld", getenv("USER"), (long) time(0));
		}
	}
	create_dir(scratch_dir, 0755);
	debug(D_DEBUG, "scratch dir: %s", scratch_dir);

	// get absolute path for pool config file
	if(auto_worker_pool) {
		struct stat tmp_stat;
		if(stat(pool_config_path, &tmp_stat) != 0) {
			fprintf(stderr, "Error: failed to locate expected pool configuration file - %s (%s).\n", pool_config_path, strerror(errno));
			return EXIT_FAILURE;
		}

		// get pool pid file's absolute path (for later delete this file while in the scratch dir
		strncpy(pool_pid_canonical_path, starting_dir_canonical_path, PATH_MAX);
		strncat(pool_pid_canonical_path, pidfile_path, strlen(pidfile_path));
		
		if(make_decision_only) {
			// get pool pid file's absolute path (for later delete this file while in the scratch dir
			strncpy(pool_name_canonical_path, starting_dir_canonical_path, PATH_MAX);
			strncat(pool_name_canonical_path, poolnamefile_path, strlen(poolnamefile_path));
		}

		if(pool_config_path[0] == '/') {
			strncpy(pool_config_canonical_path, pool_config_path, PATH_MAX);
		} else {
			p = pool_config_path;
			if(strlen(pool_config_path) > 2) {
				if(!strncmp(pool_config_path, "./", 2)) {
					p = pool_config_path + 2;
				}
			}
			strncpy(pool_config_canonical_path, starting_dir_canonical_path, PATH_MAX);
			strncat(pool_config_canonical_path, p, strlen(p));
		}
	}

	// Copy the worker program to the tmp directory and we will enter that
	// directory afterwards. We have to copy the worker program to a local
	// filesystem (other than afs, etc.) because condor might not be able
	// to access your shared file system
	snprintf(new_worker_path, PATH_MAX, "%s/work_queue_worker", scratch_dir);
	if(!copy_executable(worker_path, new_worker_path)) {
		return EXIT_FAILURE;
	}

	if(workers_per_job) {
		snprintf(new_pool_path, PATH_MAX, "%s/work_queue_pool", scratch_dir);
		if(!copy_executable(pool_path, new_pool_path)) {
			return EXIT_FAILURE;
		}
	}

	// ATTENTION: switch to the scratch dir.
	if(chdir(scratch_dir) < 0) {
		fprintf(stderr, "Unable to cd into scratch directory %s: %s\n", scratch_dir, strerror(errno));
		return EXIT_FAILURE;
	}
	printf("scratch directory: %s\n", scratch_dir);

	// Set start worker command and specify the required input files
	if(!workers_per_job) {
		snprintf(worker_cmd, PATH_MAX, "./work_queue_worker %s %s", worker_args, extra_worker_args);
		snprintf(worker_input_files, PATH_MAX, "work_queue_worker");
	} else {
		// Create multiple local workers
		snprintf(worker_cmd, PATH_MAX, "./work_queue_pool %s -E %s %d", worker_args, extra_worker_args, workers_per_job);
		snprintf(worker_input_files, PATH_MAX, "work_queue_worker,work_queue_pool");
	}

	q = batch_queue_create(batch_queue_type);
	if(!q) {
		fatal("Unable to create batch queue of type: %s", batch_queue_type_to_string(batch_queue_type));
	}
	batch_queue_set_options(q, getenv("BATCH_OPTIONS"));
	job_table = itable_create(0);

	// The worker pool now works in one of the following 3 modes:
	if(guarantee_x_running_workers_and_quit) {
		// 1. guarantee X (as user specified) workers are running (as opposed
		// to submitted) and then quit. This is useful for performance
		// experiments when you need a fixed amount of workers.
		start_x_running_workers(batch_queue_type, worker_cmd, worker_input_files, goal);
	} else if(auto_worker_pool) {
		// 2. automatically allocate workers for masters based on worker pool policy
		start_serving_masters(catalog_host, catalog_port, pool_config_canonical_path);
	} else {
		// 3. maintain a fixed amount of submitted workers
		maintain_x_submitted_workers(worker_cmd, worker_input_files, goal);
	}

	// Abort all jobs
	if(logfile) {
		fclose(logfile);
	}
	if(regex_list) {
		list_free(regex_list);
		list_delete(regex_list);
	}
	remove_workers(job_table);
	printf("All workers aborted.\n");
	delete_dir(scratch_dir);
	if(pool_pid_canonical_path[0]) {
		unlink(pool_pid_canonical_path);
	}
	if(make_decision_only && pool_name_canonical_path[0]) {
		unlink(pool_name_canonical_path);
	}
	batch_queue_delete(q);

	return EXIT_SUCCESS;
}

/* vim: set noexpandtab tabstop=4: */
