/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"

#include "int_sizes.h"
#include "link.h"
#include "debug.h"
#include "stringtools.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "datagram.h"
#include "domain_name_cache.h"
#include "hash_table.h"
#include "itable.h"
#include "list.h"
#include "macros.h"
#include "process.h"
#include "username.h"
#include "create_dir.h"
#include "xmalloc.h"

#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <errno.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>

#ifdef CCTOOLS_OPSYS_SUNOS
extern int setenv( const char *name, const char *value, int overwrite );
#endif

#define WORKER_STATE_INIT  0
#define WORKER_STATE_READY 1
#define WORKER_STATE_BUSY  2
#define WORKER_STATE_NONE  3
#define WORKER_STATE_MAX   (WORKER_STATE_NONE+1)

#define WORK_QUEUE_FILE 0
#define WORK_QUEUE_BUFFER 1
#define WORK_QUEUE_REMOTECMD 2

#define TASK_STATUS_INITIALIZING 0
#define TASK_STATUS_SENDING_INPUT 1
#define TASK_STATUS_EXECUTING 2
#define TASK_STATUS_RECEIVING_OUTPUT 3

double wq_option_fast_abort_multiplier = -1.0;
int wq_option_scheduler = WORK_QUEUE_SCHEDULE_DEFAULT;
static struct datagram *outgoing_datagram = NULL;
static time_t catalog_update_time;
int wq_tolerable_transfer_time_multiplier = 10;
int wq_minimum_transfer_timeout = 3;

struct work_queue {
	char *name;
	int master_mode;
	int worker_mode;
	int priority;
	struct link *master_link;
	struct list *ready_list;
	struct list *complete_list;
	struct hash_table *worker_table;
	struct link_info *poll_table;
	int poll_table_size;
	int workers_in_state[WORKER_STATE_MAX];
	INT64_T total_tasks_submitted;
	INT64_T total_tasks_complete;
	INT64_T total_task_time;
	INT64_T total_workers_joined;
	INT64_T total_workers_removed;
	INT64_T total_bytes_sent;
	INT64_T total_bytes_received;
	timestamp_t total_send_time;
	timestamp_t total_receive_time;
	double fast_abort_multiplier;
	int worker_selection_algorithm;		  /**< How to choose worker to run the task. */
};


struct work_queue_worker {
	int state;
	char hostname[DOMAIN_NAME_MAX];
	char os[65];
	char arch[65];
	char addrport[32];
	char hashkey[32];
	int ncpus;
	INT64_T memory_avail;
	INT64_T memory_total;
	INT64_T disk_avail;
	INT64_T disk_total;
	struct hash_table *current_files;
	struct link *link;
	struct work_queue_task *current_task;
	INT64_T total_tasks_complete;
	timestamp_t total_task_time;
	INT64_T total_bytes_transferred;
	timestamp_t total_transfer_time;
};

struct work_queue_file {
	int type;		// WORK_QUEUE_FILE or WORK_QUEUE_BUFFER
	int flags;		// WORK_QUEUE_CACHE or others in the future.
	int length;		// length of payload
	void *payload;		// name on master machine or buffer of data.
	char *remote_name;	// name on remote machine.
};

static int start_one_task(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t);
static int start_task_on_worker(struct work_queue *q, struct work_queue_worker *w);
static int update_catalog(struct work_queue *q);
static timestamp_t get_transfer_wait_time(struct work_queue *q, struct work_queue_worker *w, INT64_T length);

static int short_timeout = 5;
static int next_taskid = 1;

struct work_queue_task *work_queue_task_create(const char *command_line)
{
	struct work_queue_task *t = malloc(sizeof(*t));
	memset(t, 0, sizeof(*t));
	t->command_line = strdup(command_line);
	t->tag = NULL;
	t->worker_selection_algorithm = WORK_QUEUE_SCHEDULE_UNSET;
	t->output = NULL;
	t->input_files = list_create();
	t->output_files = list_create();
	t->return_status = WORK_QUEUE_RETURN_STATUS_UNSET;
	t->result = WORK_QUEUE_RESULT_UNSET;
	t->taskid = next_taskid++;

	t->submit_time = t->start_time = t->finish_time = t->transfer_start_time = t->total_transfer_time = t->computation_time = 0;

	t->total_bytes_transferred = 0;
	t->status = TASK_STATUS_INITIALIZING;
	return t;
}

void work_queue_task_delete(struct work_queue_task *t)
{
	struct work_queue_file *tf;
	if(t) {
		if(t->command_line)
			free(t->command_line);
		if(t->tag)
			free(t->tag);
		if(t->output)
			free(t->output);
		if(t->input_files) {
			while((tf = list_pop_tail(t->input_files))) {
				if(tf->payload)
					free(tf->payload);
				if(tf->remote_name)
					free(tf->remote_name);
				free(tf);
			}
			list_delete(t->input_files);
		}
		if(t->output_files) {
			while((tf = list_pop_tail(t->output_files))) {
				if(tf->payload)
					free(tf->payload);
				if(tf->remote_name)
					free(tf->remote_name);
				free(tf);
			}
			list_delete(t->output_files);
		}
		if(t->host)
			free(t->host);
		free(t);
	}
}

static void change_worker_state(struct work_queue *q, struct work_queue_worker *w, int state)
{
	q->workers_in_state[w->state]--;
	w->state = state;
	q->workers_in_state[state]++;
	if(q->master_mode == WORK_QUEUE_MASTER_MODE_CATALOG) {
		update_catalog(q);
		catalog_update_time = time(0);
	}
	debug(D_WQ, "Number of workers in state 'busy': %d;  'ready': %d", q->workers_in_state[WORKER_STATE_BUSY], q->workers_in_state[WORKER_STATE_READY]);
}

static void link_to_hash_key(struct link *link, char *key)
{
	sprintf(key, "0x%p", link);
}

void work_queue_get_stats(struct work_queue *q, struct work_queue_stats *s)
{
	memset(s, 0, sizeof(*s));
	s->workers_init = q->workers_in_state[WORKER_STATE_INIT];
	s->workers_ready = q->workers_in_state[WORKER_STATE_READY];
	s->workers_busy = q->workers_in_state[WORKER_STATE_BUSY];
	s->tasks_waiting = list_size(q->ready_list);
	s->tasks_running = q->workers_in_state[WORKER_STATE_BUSY];
	s->tasks_complete = list_size(q->complete_list);
	s->total_tasks_dispatched = q->total_tasks_submitted;
	s->total_tasks_complete = q->total_tasks_complete;
	s->total_workers_joined = q->total_workers_joined;
	s->total_workers_removed = q->total_workers_removed;
	s->total_bytes_sent = q->total_bytes_sent;
	s->total_bytes_received = q->total_bytes_received;
	s->total_send_time = q->total_send_time;
	s->total_receive_time = q->total_receive_time;
}

static void add_worker(struct work_queue *q)
{
	struct link *link;
	struct work_queue_worker *w;
	char addr[LINK_ADDRESS_MAX];
	int port;

	link = link_accept(q->master_link, time(0) + short_timeout);
	if(link) {
		link_tune(link, LINK_TUNE_INTERACTIVE);
		if(link_address_remote(link, addr, &port)) {
			w = malloc(sizeof(*w));
			memset(w, 0, sizeof(*w));
			w->state = WORKER_STATE_NONE;
			w->link = link;
			w->current_files = hash_table_create(0, 0);
			link_to_hash_key(link, w->hashkey);
			sprintf(w->addrport, "%s:%d", addr, port);
			hash_table_insert(q->worker_table, w->hashkey, w);
			change_worker_state(q, w, WORKER_STATE_INIT);
			debug(D_WQ, "worker %s added", w->addrport);
			debug(D_WQ, "%d workers are connected in total now", hash_table_size(q->worker_table));
			q->total_workers_joined++;
		} else {
			link_close(link);
		}
	}
}

static void remove_worker(struct work_queue *q, struct work_queue_worker *w)
{
	char *key, *value;
	struct work_queue_task *t;

	debug(D_WQ, "worker %s removed", w->addrport);
	q->total_workers_removed++;

	hash_table_firstkey(w->current_files);
	while(hash_table_nextkey(w->current_files, &key, (void **) &value)) {
		hash_table_remove(w->current_files, key);
		free(value);
	}
	hash_table_delete(w->current_files);

	hash_table_remove(q->worker_table, w->hashkey);
	t = w->current_task;
	if(t) {
		if(t->result & WORK_QUEUE_RESULT_INPUT_MISSING || t->result & WORK_QUEUE_RESULT_OUTPUT_MISSING || t->result & WORK_QUEUE_RESULT_FUNCTION_FAIL) {
			list_push_head(q->complete_list, w->current_task);
		} else {
			t->result = WORK_QUEUE_RESULT_UNSET;
			t->total_bytes_transferred = 0;
			t->total_transfer_time = 0;
			list_push_head(q->ready_list, w->current_task);
		}
		w->current_task = 0;
	}
	change_worker_state(q, w, WORKER_STATE_NONE);
	if(w->link)
		link_close(w->link);
	free(w);

	debug(D_WQ, "%d workers are connected in total now", hash_table_size(q->worker_table));
}

/**
 * This function implements the "rget %s" protocol.
 * It reads a streamed item from a worker. For the stream format, please refer
 * to the stream_output_item function in worker.c
 */
static int get_output_item(char *remote_name, char *local_name, struct work_queue *q, struct work_queue_worker *w, struct hash_table *received_items, INT64_T * total_bytes)
{
	char line[WORK_QUEUE_CATALOG_LINE_MAX];
	int fd;
	INT64_T actual, length;
	time_t stoptime;
	char type[256];
	char tmp_remote_name[WORK_QUEUE_CATALOG_LINE_MAX], tmp_local_name[WORK_QUEUE_CATALOG_LINE_MAX];
	char *cur_pos, *tmp_pos;
	int remote_name_len;
	int local_name_len;

	if(hash_table_lookup(received_items, local_name))
		return 1;

	debug(D_WQ, "%s (%s) sending back %s to %s", w->hostname, w->addrport, remote_name, local_name);
	link_putfstring(w->link, "rget %s\n", time(0) + short_timeout, remote_name);

	strcpy(tmp_local_name, local_name);
	remote_name_len = strlen(remote_name);
	local_name_len = strlen(local_name);

	while(1) {
		if(!link_readline(w->link, line, sizeof(line), time(0) + short_timeout)) {
			goto link_failure;
		}

		if(sscanf(line, "%s %s %lld", type, tmp_remote_name, &length) == 3) {
			tmp_local_name[local_name_len] = '\0';
			strcat(tmp_local_name, &(tmp_remote_name[remote_name_len]));

			if(strncmp(type, "dir", 3) == 0) {
				debug(D_WQ, "%s (%s) dir %s", w->hostname, w->addrport, tmp_local_name);
				if(!create_dir(tmp_local_name, 0700)) {
					debug(D_WQ, "Cannot create directory - %s (%s)", tmp_local_name, strerror(errno));
					goto failure;
				}
				hash_table_insert(received_items, tmp_local_name, strdup(tmp_local_name));
			} else if(strncmp(type, "file", 4) == 0) {
				// actually place the file
				if(length >= 0) {
					// create dirs in the filename path if needed
					cur_pos = tmp_local_name;
					if(!strncmp(cur_pos, "./", 2)) {
						cur_pos += 2;
					}

					tmp_pos = strrchr(cur_pos, '/');
					while(tmp_pos) {
						*tmp_pos = '\0';
						if(!create_dir(cur_pos, 0700)) {
							debug(D_WQ, "Could not create directory - %s (%s)", cur_pos, strerror(errno));
							goto failure;
						}
						*tmp_pos = '/';

						cur_pos = tmp_pos + 1;
						tmp_pos = strrchr(cur_pos, '/');
					}

					// get the remote file and place it
					debug(D_WQ, "Receiving file %s (size: %lld bytes) from %s (%s) ...", tmp_local_name, length, w->addrport, w->hostname);
					fd = open(tmp_local_name, O_WRONLY | O_TRUNC | O_CREAT, 0700);
					if(fd < 0) {
						debug(D_NOTICE, "Cannot open file %s for writing: %s", tmp_local_name, strerror(errno));
						goto failure;
					}
					stoptime = time(0) + get_transfer_wait_time(q, w, length);
					actual = link_stream_to_fd(w->link, fd, length, stoptime);
					close(fd);
					if(actual != length) {
						debug(D_WQ, "Received item size (%lld) does not match the expected size - %lld bytes.", actual, length);
						unlink(local_name);
						goto failure;
					}
					*total_bytes += length;

					hash_table_insert(received_items, tmp_local_name, strdup(tmp_local_name));
				} else {
					debug(D_NOTICE, "%s on %s (%s) has invalid length: %lld", remote_name, w->addrport, w->hostname, length);
					goto failure;
				}
			} else if(strncmp(type, "missing", 7) == 0) {
				// now length holds the errno
				debug(D_NOTICE, "Failed to retrieve %s from %s (%s): %s", remote_name, w->addrport, w->hostname, strerror(length));
				w->current_task->result |= WORK_QUEUE_RESULT_OUTPUT_MISSING;
			} else {
				debug(D_WQ, "Invalid output item type - %s\n", type);
				goto failure;
			}
		} else if(sscanf(line, "%s", type) == 1) {
			if(strncmp(type, "end", 3) == 0) {
				break;
			} else {
				debug(D_WQ, "Invalid rget line - %s\n", line);
				goto failure;
			}
		} else {
			debug(D_WQ, "Invalid streaming output line - %s\n", line);
			goto failure;
		}
	}

	return 1;

      link_failure:
	debug(D_WQ, "Link to %s (%s) failed.\n", w->addrport, w->hostname);
	w->current_task->result |= WORK_QUEUE_RESULT_LINK_FAIL;

      failure:
	debug(D_WQ, "%s (%s) failed to return %s to %s", w->addrport, w->hostname, remote_name, local_name);
	w->current_task->result |= WORK_QUEUE_RESULT_OUTPUT_FAIL;
	return 0;
}

/**
 * Comparison function for sorting by file/dir names in the output files list
 * of a task
 */
int filename_comparator(const void *a, const void *b)
{
	int rv;
	rv = strcmp(*(char *const *) a, *(char *const *) b);
	return rv > 0 ? -1 : 1;
}

static int get_output_files(struct work_queue_task *t, struct work_queue_worker *w, struct work_queue *q)
{
	struct work_queue_file *tf;

	// This may be where I can trigger output file cache
	// Added by Anthony Canino
	// Attempting to add output cacheing
	struct stat local_info;
	struct stat *remote_info;
	char *hash_name;
	char *key, *value;
	struct hash_table *received_items;
	INT64_T total_bytes = 0;

	timestamp_t open_time = 0;
	timestamp_t close_time = 0;
	timestamp_t sum_time;

	// Start transfer ...
	t->status = TASK_STATUS_RECEIVING_OUTPUT;
	received_items = hash_table_create(0, 0);

	//  Sorting list will make sure that upper level dirs sit before their
	//  contents(files/dirs) in the output files list. So, when we emit rget
	//  command, we would first encounter top level dirs. Also, we would record
	//  every received files/dirs within those top level dirs. If any file/dir
	//  in those top level dirs appears later in the output files list, we
	//  won't transfer it again.
	list_sort(t->output_files, filename_comparator);

	if(t->output_files) {
		list_first_item(t->output_files);
		while((tf = list_next_item(t->output_files))) {
			if(tf->flags & WORK_QUEUE_THIRDPUT) {

				debug(D_WQ, "thirdputting %s as %s", tf->remote_name, tf->payload);

				if(!strcmp(tf->remote_name, tf->payload)) {
					debug(D_WQ, "output file %s already on shared filesystem", tf->remote_name);
					tf->flags |= WORK_QUEUE_PREEXIST;
				} else {
					char thirdput_result[WORK_QUEUE_LINE_MAX];
					debug(D_WQ, "putting %s from %s (%s) to shared filesystem from %s", tf->remote_name, w->hostname, w->addrport, tf->payload);
					open_time = timestamp_get();
					link_putfstring(w->link, "thirdput %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_PATH, tf->remote_name, tf->payload);
					link_readline(w->link, thirdput_result, WORK_QUEUE_LINE_MAX, time(0) + short_timeout);
					close_time = timestamp_get();
					sum_time += (close_time - open_time);
				}
			} else if(tf->type == WORK_QUEUE_REMOTECMD) {
				char thirdput_result[WORK_QUEUE_LINE_MAX];
				debug(D_WQ, "putting %s from %s (%s) to remote filesystem using %s", tf->remote_name, w->hostname, w->addrport, tf->payload);
				open_time = timestamp_get();
				link_putfstring(w->link, "thirdput %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_CMD, tf->remote_name, tf->payload);
				link_readline(w->link, thirdput_result, WORK_QUEUE_LINE_MAX, time(0) + short_timeout);
				close_time = timestamp_get();
				sum_time += (close_time - open_time);
			} else {

				open_time = timestamp_get();
				get_output_item(tf->remote_name, (char *) tf->payload, q, w, received_items, &total_bytes);
				close_time = timestamp_get();
				if(t->result & WORK_QUEUE_RESULT_OUTPUT_FAIL) {
					return 0;
				}
				if(total_bytes) {
					sum_time = close_time - open_time;
					q->total_bytes_received += total_bytes;
					q->total_receive_time += sum_time;
					t->total_bytes_transferred += total_bytes;
					t->total_transfer_time += sum_time;
					w->total_bytes_transferred += total_bytes;
					w->total_transfer_time += sum_time;
				}
				total_bytes = 0;
			}


			// Add the output item to the hash table if its cacheable
			if(tf->flags & WORK_QUEUE_CACHE) {
				if(stat(tf->payload, &local_info) < 0) {
					unlink(tf->payload);
					if(t->result & WORK_QUEUE_RESULT_OUTPUT_MISSING)
						continue;
					return 0;
				}

				hash_name = (char *) malloc((strlen(tf->payload) + strlen(tf->remote_name) + 2) * sizeof(char));
				sprintf(hash_name, "%s-%s", (char *) tf->payload, tf->remote_name);

				remote_info = malloc(sizeof(*remote_info));
				memcpy(remote_info, &local_info, sizeof(local_info));
				hash_table_insert(w->current_files, hash_name, remote_info);
			}

		}

	}
	// destroy received files hash table for this task
	hash_table_firstkey(received_items);
	while(hash_table_nextkey(received_items, &key, (void **) &value)) {
		free(value);
	}
	hash_table_delete(received_items);

	return 1;
}

static void delete_uncacheable_files(struct work_queue_task *t, struct work_queue_worker *w)
{
	struct work_queue_file *tf;

	if(t->input_files) {
		list_first_item(t->input_files);
		while((tf = list_next_item(t->input_files))) {
			if(!(tf->flags & WORK_QUEUE_CACHE) && !(tf->flags & WORK_QUEUE_PREEXIST)) {
				debug(D_WQ, "%s (%s) unlink %s", w->hostname, w->addrport, tf->remote_name);
				link_putfstring(w->link, "unlink %s\n", time(0) + short_timeout, tf->remote_name);
			}
		}
	}

	if(t->output_files) {
		list_first_item(t->output_files);
		while((tf = list_next_item(t->output_files))) {
			if(!(tf->flags & WORK_QUEUE_CACHE) && !(tf->flags & WORK_QUEUE_PREEXIST)) {
				debug(D_WQ, "%s (%s) unlink %s", w->hostname, w->addrport, tf->remote_name);
				link_putfstring(w->link, "unlink %s\n", time(0) + short_timeout, tf->remote_name);
			}
		}
	}
}

static int match_project_names(struct work_queue *q, const char *project_names)
{
	char *str;
	char *token = NULL;
	char *delims = " \t";

	if(!q || !project_names)
		return 0;

	str = xstrdup(project_names);
	token = strtok(str, delims);
	while(token) {
		// token holds the preferred master name pattern
		debug(D_WQ, "Matching %s against %s", q->name, project_names);
		if(whole_string_match_regex(q->name, token)) {
			free(str);
			return 1;
		}

		token = strtok(NULL, delims);
	}
	free(str);
	return 0;
}

static int handle_worker(struct work_queue *q, struct link *l)
{
	char line[WORK_QUEUE_CATALOG_LINE_MAX];
	char key[WORK_QUEUE_CATALOG_LINE_MAX];
	struct work_queue_worker *w;
	int result, output_length;
	time_t stoptime;

	char project_names[WORK_QUEUE_LINE_MAX];

	link_to_hash_key(l, key);
	w = hash_table_lookup(q->worker_table, key);

	if(link_readline(l, line, sizeof(line), time(0) + short_timeout)) {
		if(sscanf(line, "ready %s %d %lld %lld %lld %lld", w->hostname, &w->ncpus, &w->memory_avail, &w->memory_total, &w->disk_avail, &w->disk_total)== 6) {
			// More workers than needed are connected
			int workers_connected = hash_table_size(q->worker_table);
			int jobs_not_completed = list_size(q->ready_list) + q->workers_in_state[WORKER_STATE_BUSY];
			if(workers_connected > jobs_not_completed) {
				debug(D_WQ, "Jobs waiting + running: %d; Workers connected now: %d", jobs_not_completed, workers_connected);
				goto reject;
			}
			
			if(q->worker_mode == WORK_QUEUE_WORKER_MODE_EXCLUSIVE && q->name) {
				// For backward compatibility, we scan the line AGAIN to see if it contains extra fields
				if(sscanf(line, "ready %*s %*d %*d %*d %*d %*d \"%[^\"]\"", project_names) == 1) {
					if(!match_project_names(q, project_names)) {
						debug(D_WQ, "Preferred masters of %s (%s): %s", w->hostname, w->addrport, project_names);
						goto reject;
					}
				} else {
					// No extra fields, so this is a shared worker
					debug(D_WQ, "%s (%s) is a shared worker. But the master does not allow shared workers.", w->hostname, w->addrport);
					goto reject;
				}
			}

			//Re-scan to see if worker reports its os and arch.
			if(sscanf(line, "ready %*s %*d %*d %*d %*d %*d \"%[^\"]\"", project_names) == 1) {
				if(sscanf(line, "ready %*s %*d %*d %*d %*d %*d \"%*[^\"]\" %s %s", w->os, w->arch) != 2) {
					strcpy(w->os, "unknown");
					strcpy(w->arch, "unknown");
				}
			} else {
				//check in exclusive worker with no preferred project which it sends as a blank ""
				if(sscanf(line, "ready %*s %*d %*d %*d %*d %*d \"\" %s %s", w->os, w->arch) != 2) {
					//check in shared worker
					if(sscanf(line, "ready %*s %*d %*d %*d %*d %*d %s %s", w->os, w->arch) != 2) {
						strcpy(w->os, "unknown");
						strcpy(w->arch, "unknown");
					}
				}	
			}
			
			if(w->state == WORKER_STATE_INIT) {
				change_worker_state(q, w, WORKER_STATE_READY);
				debug(D_WQ, "%s (%s) running %s on %s is ready", w->hostname, w->addrport, w->os, w->arch);
			}
		} else if(sscanf(line, "result %d %d", &result, &output_length) == 2) {
			struct work_queue_task *t = w->current_task;
			int actual;

			t->computation_time = timestamp_get() - t->start_time;
			t->output = malloc(output_length + 1);

			if(output_length > 0) {
				//stoptime = time(0) + MAX(1.0,output_length/1250000.0);
				stoptime = time(0) + get_transfer_wait_time(q, w, (INT64_T) output_length);
				actual = link_read(l, t->output, output_length, stoptime);
				if(actual != output_length) {
					free(t->output);
					t->output = 0;
					goto failure;
				}
			} else {
				actual = 0;
			}
			t->output[actual] = 0;

			t->return_status = result;
			if(t->return_status != 0)
				t->result |= WORK_QUEUE_RESULT_FUNCTION_FAIL;

			if(!get_output_files(t, w, q)) {
				free(t->output);
				t->output = 0;
				goto failure;
			}

			delete_uncacheable_files(t, w);

			// Record current task as completed and change worker's state
			list_push_head(q->complete_list, w->current_task);
			w->current_task = 0;
			change_worker_state(q, w, WORKER_STATE_READY);

			t->finish_time = timestamp_get();

			t->host = strdup(w->addrport);
			q->total_tasks_complete++;
			q->total_task_time += (t->finish_time - t->start_time);
			w->total_tasks_complete++;
			w->total_task_time += (t->finish_time - t->start_time);
			debug(D_WQ, "%s (%s) done in %.02lfs total tasks %d average %.02lfs", w->hostname, w->addrport, (t->finish_time - t->transfer_start_time) / 1000000.0, w->total_tasks_complete,
			      w->total_task_time / w->total_tasks_complete / 1000000.0);

			start_task_on_worker(q, w);
		} else {
			goto failure;
		}
	} else {
		goto failure;
	}

	return 1;

      reject:
	debug(D_NOTICE, "%s (%s) is rejected and removed.", w->hostname, w->addrport);
	remove_worker(q, w);
	return 0;

      failure:
	debug(D_NOTICE, "%s (%s) failed and removed.", w->hostname, w->addrport);
	remove_worker(q, w);
	return 0;
}

static int build_poll_table(struct work_queue *q)
{
	int n = 1;
	char *key;
	struct work_queue_worker *w;

	// Allocate a small table, if it hasn't been done yet.
	if(!q->poll_table) {
		q->poll_table = malloc(sizeof(*q->poll_table) * q->poll_table_size);
	}
	// The first item in the poll table is the master link, which accepts new connections.
	q->poll_table[0].link = q->master_link;
	q->poll_table[0].events = LINK_READ;
	q->poll_table[0].revents = 0;

	// For every worker in the hash table, add an item to the poll table
	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {

		// If poll table is not large enough, reallocate it
		if(n >= q->poll_table_size) {
			q->poll_table_size *= 2;
			q->poll_table = realloc(q->poll_table, sizeof(*q->poll_table) * q->poll_table_size);
		}

		q->poll_table[n].link = w->link;
		q->poll_table[n].events = LINK_READ;
		q->poll_table[n].revents = 0;
		n++;
	}

	return n;
}

static int put_file(struct work_queue_file *, const char *, struct work_queue *, struct work_queue_worker *, INT64_T *);

static int put_directory(const char *dirname, const char *remote_name, struct work_queue *q, struct work_queue_worker *w, INT64_T * total_bytes)
{
	DIR *dir = opendir(dirname);
	if(!dir)
		return 0;

	struct dirent *file;
	struct work_queue_file tf;

	char buffer[WORK_QUEUE_CATALOG_LINE_MAX];

	while((file = readdir(dir))) {
		char *filename = file->d_name;
		int len;

		if(!strcmp(filename, ".") || !strcmp(filename, "..")) {
			continue;
		}

		tf.type = WORK_QUEUE_FILE;
		tf.flags = WORK_QUEUE_CACHE;
		
		*buffer = '\0';
		len = sprintf(buffer, "%s/%s", dirname, filename);
		tf.length = len;
		tf.payload = strdup(buffer);
		
		*buffer = '\0';
		len = sprintf(buffer, "%s/%s", remote_name, filename);
		tf.remote_name = strdup(buffer);

		if(!put_file(&tf, NULL, q, w, total_bytes))
			return 0;
	}

	closedir(dir);
	return 1;
}

static int put_file(struct work_queue_file *tf, const char *expanded_payload, struct work_queue *q, struct work_queue_worker *w, INT64_T * total_bytes)
{
	struct stat local_info;
	struct stat *remote_info;
	char *hash_name;
	time_t stoptime;
	INT64_T actual = 0;
	int dir = 0;
	char *payload;

	if (expanded_payload){
		payload = xstrdup(expanded_payload);
	}
	else {
		payload = xstrdup(tf->payload);
	}

	if(stat(payload, &local_info) < 0)
		return 0;
	if(local_info.st_mode & S_IFDIR)
		dir = 1;
	/* normalize the mode so as not to set up invalid permissions */
	if(dir) {
		local_info.st_mode |= 0700;
	} else {
		local_info.st_mode |= 0600;
	}
	local_info.st_mode &= 0777;

	hash_name = (char *) malloc((strlen(payload) + strlen(tf->remote_name) + 2) * sizeof(char));
	sprintf(hash_name, "%s-%s", payload, tf->remote_name);

	remote_info = hash_table_lookup(w->current_files, hash_name);

	if(!remote_info || remote_info->st_mtime != local_info.st_mtime || remote_info->st_size != local_info.st_size) {
		if(remote_info) {
			hash_table_remove(w->current_files, hash_name);
			free(remote_info);
		}

		debug(D_WQ, "%s (%s) needs file %s", w->hostname, w->addrport, payload);
		if(dir) {
			// If mkdir fails, the future calls to 'put_file' function to place
			// files in that directory won't suceed. Such failure would
			// eventually be captured in 'start_tasks' function and in the
			// 'start_tasks' function the corresponding worker would be removed.
			link_putfstring(w->link, "mkdir %s %o\n", time(0) + short_timeout, tf->remote_name, local_info.st_mode);

			if(!put_directory(payload, tf->remote_name, q, w, total_bytes))
				return 0;

			return 1;
		}

		int fd = open(payload, O_RDONLY, 0);
		if(fd < 0)
			return 0;

		stoptime = time(0) + get_transfer_wait_time(q, w, (INT64_T) local_info.st_size);
		link_putfstring(w->link, "put %s %lld 0%o\n", time(0) + short_timeout, tf->remote_name, (INT64_T) local_info.st_size, local_info.st_mode);
		actual = link_stream_from_fd(w->link, fd, local_info.st_size, stoptime);
		close(fd);

		if(actual != local_info.st_size)
			return 0;

		if(tf->flags & WORK_QUEUE_CACHE) {
			remote_info = malloc(sizeof(*remote_info));
			memcpy(remote_info, &local_info, sizeof(local_info));
			hash_table_insert(w->current_files, hash_name, remote_info);
		}

		*total_bytes += actual;
	}

	free(payload);
	free(hash_name);
	return 1;
}

/** 
 *	This function expands Work Queue environment variables such as
 * 	$OS, $ARCH, that are specified in the definition of Work Queue 
 * 	input files. It expands these variables based on the info reported 
 *	by each connected worker.
 *	Will always return a non-empty string. That is if no match is found
 *	for any of the environment variables, it will return the input string
 *	as is.
 * 	*/
char *expand_envnames(struct work_queue_worker *w, const char *payload)
{
	char *expanded_name;
	char *str, *curr_pos;
	char *delimtr = "$";
	char *token;

	str = xstrdup(payload);

	expanded_name = (char *)malloc(strlen(payload) + (50 * sizeof(char)));
	if (expanded_name == NULL){
		return NULL;
	}
	else {
		//Initialize to null byte so it works correctly with strcat.
		*expanded_name = '\0';
	}
	
	token = strtok(str, delimtr);
	while(token) {
		if ((curr_pos = strstr(token, "ARCH"))){
			if ((curr_pos - token) == 0){
				strcat(expanded_name, w->arch);
				strcat(expanded_name, token+4);
			}
			else {
				//No match. So put back '$' and rest of the string.
				strcat(expanded_name, "$");
				strcat(expanded_name, token);
			}
		}
		else if ((curr_pos = strstr(token, "OS"))){
			if ((curr_pos - token) == 0){
				if (strstr(w->os, "CYGWIN")) {
					strcat(expanded_name, "Cygwin");
				}
				else {
					strcat(expanded_name, w->os);
				}		  		
				strcat(expanded_name, token+2);
			}
			else {
				strcat(expanded_name, "$");
				strcat(expanded_name, token);
			}
		}
		else {
			//Put back '$' only if it does not appear at start of the string.
			if ((token - str) > 0) {
				strcat(expanded_name, "$");
			}	
			strcat(expanded_name, token);
		}
		token = strtok(NULL, delimtr);
	}

	free(str);
	return expanded_name;
}

static int send_input_files(struct work_queue_task *t, struct work_queue_worker *w, struct work_queue *q)
{
	struct work_queue_file *tf;
	int actual = 0;
	INT64_T total_bytes = 0;
	timestamp_t open_time = 0;
	timestamp_t close_time = 0;
	timestamp_t sum_time = 0;
	int fl;
	time_t stoptime;
	struct stat s;
	char *expanded_payload = NULL;
	t->status = TASK_STATUS_SENDING_INPUT;
		
	// Check input existence
	if(t->input_files) {
		list_first_item(t->input_files);
		while((tf = list_next_item(t->input_files))) {
			if(tf->type == WORK_QUEUE_FILE) {
				if (strchr(tf->payload, '$')){
					expanded_payload = expand_envnames(w, tf->payload);
					debug(D_WQ, "File name %s expanded to %s for %s (%s).", tf->payload, expanded_payload, w->hostname, w->addrport);
				}
				else {
					expanded_payload = xstrdup(tf->payload);
				}
				if(stat(expanded_payload, &s) != 0) {
					fprintf(stderr, "Could not stat %s. (%s)\n", expanded_payload, strerror(errno));
					free(expanded_payload);
					goto failure;
				}
				free(expanded_payload);
			}
		}
	}
	// Start transfer ...
	if(t->input_files) {
		list_first_item(t->input_files);
		while((tf = list_next_item(t->input_files))) {
			if(tf->type == WORK_QUEUE_BUFFER) {
				debug(D_WQ, "%s (%s) needs literal as %s", w->hostname, w->addrport, tf->remote_name);
				fl = tf->length;

				stoptime = time(0) + get_transfer_wait_time(q, w, (INT64_T) fl);
				open_time = timestamp_get();
				link_putfstring(w->link, "put %s %lld %o\n", time(0) + short_timeout, tf->remote_name, (INT64_T) fl, 0777);
				actual = link_putlstring(w->link, tf->payload, fl, stoptime);
				close_time = timestamp_get();
				if(actual != (fl))
					goto failure;
				total_bytes += actual;
				sum_time += (close_time - open_time);
			} else if(tf->type == WORK_QUEUE_REMOTECMD) {
				debug(D_WQ, "%s (%s) needs %s from remote filesystem using %s", w->hostname, w->addrport, tf->remote_name, tf->payload);
				open_time = timestamp_get();
				link_putfstring(w->link, "thirdget %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_CMD, tf->remote_name, tf->payload);
				close_time = timestamp_get();
				sum_time += (close_time - open_time);
			} else {
				if(tf->flags & WORK_QUEUE_THIRDGET) {
					debug(D_WQ, "%s (%s) needs %s from shared filesystem as %s", w->hostname, w->addrport, tf->payload, tf->remote_name);

					if(!strcmp(tf->remote_name, tf->payload)) {
						tf->flags |= WORK_QUEUE_PREEXIST;
					} else {
						open_time = timestamp_get();
						if(tf->flags & WORK_QUEUE_SYMLINK) {
							link_putfstring(w->link, "thirdget %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_SYMLINK, tf->remote_name, tf->payload);
						} else {
							link_putfstring(w->link, "thirdget %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_PATH, tf->remote_name, tf->payload);
						}
						close_time = timestamp_get();
						sum_time += (close_time - open_time);
					}
				} else {				
					open_time = timestamp_get();
					if (strchr(tf->payload, '$')){
						expanded_payload = expand_envnames(w, tf->payload);
					}
					else {
						expanded_payload = xstrdup(tf->payload);
					}
					if(!put_file(tf, expanded_payload, q, w, &total_bytes)) {
						free(expanded_payload);
						goto failure;
					}	
					free(expanded_payload);
					close_time = timestamp_get();
					sum_time += (close_time - open_time);
				}
			}
		}
		t->total_bytes_transferred += total_bytes;
		t->total_transfer_time += sum_time;
		w->total_bytes_transferred += total_bytes;
		w->total_transfer_time += sum_time;
		if(total_bytes > 0) {
			q->total_bytes_sent += (INT64_T) total_bytes;
			q->total_send_time += sum_time;
			debug(D_WQ, "%s (%s) got %d bytes in %.03lfs (%.02lfs Mbps) average %.02lfs Mbps", w->hostname, w->addrport, total_bytes, sum_time / 1000000.0, ((8.0 * total_bytes) / sum_time),
			      (8.0 * w->total_bytes_transferred) / w->total_transfer_time);
		}
	}

	return 1;

      failure:
	if(tf->type == WORK_QUEUE_FILE)
		debug(D_WQ, "%s (%s) failed to send %s (%i bytes received).", w->hostname, w->addrport, tf->payload, actual);
	else
		debug(D_WQ, "%s (%s) failed to send literal data (%i bytes received).", w->hostname, w->addrport, actual);
	t->result |= WORK_QUEUE_RESULT_INPUT_FAIL;
	return 0;
}

int start_one_task(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t)
{
	t->transfer_start_time = timestamp_get();
	if(!send_input_files(t, w, q))
		return 0;
	t->start_time = timestamp_get();
	link_putfstring(w->link, "work %zu\n%s", time(0) + short_timeout, strlen(t->command_line), t->command_line);
	t->status = TASK_STATUS_EXECUTING;
	debug(D_WQ, "%s (%s) busy on '%s'", w->hostname, w->addrport, t->command_line);
	return 1;
}

struct work_queue_worker *find_worker_by_files(struct work_queue *q, struct work_queue_task *t)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	INT64_T most_task_cached_bytes = 0;
	INT64_T task_cached_bytes;
	struct stat *remote_info;
	struct work_queue_file *tf;
	char *hash_name;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->state == WORKER_STATE_READY) {
			task_cached_bytes = 0;
			list_first_item(t->input_files);
			while((tf = list_next_item(t->input_files))) {
				if(tf->type == WORK_QUEUE_FILE && (tf->flags & WORK_QUEUE_CACHE)) {
					hash_name = malloc((strlen(tf->payload) + strlen(tf->remote_name) + 2) * sizeof(char));
					sprintf(hash_name, "%s-%s", (char *) tf->payload, tf->remote_name);
					remote_info = hash_table_lookup(w->current_files, hash_name);
					if(remote_info)
						task_cached_bytes += remote_info->st_size;
					free(hash_name);
				}
			}

			if(!best_worker || task_cached_bytes > most_task_cached_bytes) {
				best_worker = w;
				most_task_cached_bytes = task_cached_bytes;
			}
		}
	}

	return best_worker;
}

struct work_queue_worker *find_worker_by_fcfs(struct work_queue *q)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->state == WORKER_STATE_READY)
			return w;
	}

	return best_worker;
}

struct work_queue_worker *find_worker_by_random( struct work_queue *q )
{
        char *key;
        struct work_queue_worker *w;
        struct work_queue_worker *best_worker = 0;
	struct work_queue_stats qs;
	int num_workers_ready;
	int random_ready_worker, ready_worker_count = 1;

	srand(time(0));
        
	work_queue_get_stats(q, &qs);
	num_workers_ready = qs.workers_ready;

	if (num_workers_ready > 0) {
		random_ready_worker = (rand() % num_workers_ready) + 1;
	} else { 	
  		random_ready_worker = 0;
        }     

        hash_table_firstkey(q->worker_table);
        while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
                if(w->state==WORKER_STATE_READY && ready_worker_count==random_ready_worker) {
			return w;
		}
		if (w->state==WORKER_STATE_READY){
			ready_worker_count++;
		}
	}

        return best_worker;
}

struct work_queue_worker *find_worker_by_time(struct work_queue *q)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	double best_time = HUGE_VAL;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->state == WORKER_STATE_READY) {
			if(w->total_tasks_complete > 0) {
				double t = (w->total_task_time + w->total_transfer_time) / w->total_tasks_complete;
				if(!best_worker || t < best_time) {
					best_worker = w;
					best_time = t;
				}
			}
		}
	}

	if(best_worker) {
		return best_worker;
	} else {
		return find_worker_by_fcfs(q);
	}
}

// use task-specific algorithm if set, otherwise default to the queue's setting.
struct work_queue_worker *find_best_worker(struct work_queue *q, struct work_queue_task *t)
{
	int a = t->worker_selection_algorithm;

	if(a == WORK_QUEUE_SCHEDULE_UNSET) {
		a = q->worker_selection_algorithm;
	}

	switch (a) {
	case WORK_QUEUE_SCHEDULE_FILES:
		return find_worker_by_files(q, t);
	case WORK_QUEUE_SCHEDULE_TIME:
		return find_worker_by_time(q);
	case WORK_QUEUE_SCHEDULE_RAND:
		return find_worker_by_random(q);
	case WORK_QUEUE_SCHEDULE_FCFS:
	default:
		return find_worker_by_fcfs(q);
	}
}

static int start_task_on_worker(struct work_queue *q, struct work_queue_worker *w)
{
	struct work_queue_task *t = list_pop_head(q->ready_list);
	if(!t)
		return 0;

	w->current_task = t;

	if(start_one_task(q, w, t)) {
		change_worker_state(q, w, WORKER_STATE_BUSY);
		return 1;
	} else {
		debug(D_WQ, "%s (%s) removed because couldn't send task.", w->hostname, w->addrport);
		remove_worker(q, w);	// puts w->current_task back into q->ready_list
		return 0;
	}
}

static void start_tasks(struct work_queue *q)
{
	struct work_queue_task *t;
	struct work_queue_worker *w;

	while(list_size(q->ready_list)) {
		t = list_peek_head(q->ready_list);
		w = find_best_worker(q, t);
		if(w) {
			start_task_on_worker(q, w);
		} else {
			break;
		}
	}
}

int work_queue_activate_fast_abort(struct work_queue *q, double multiplier)
{
	if(multiplier >= 1) {
		q->fast_abort_multiplier = multiplier;
		return 0;
	} else if(multiplier < 0) {
		q->fast_abort_multiplier = multiplier;
		return 0;
	} else {
		q->fast_abort_multiplier = -1.0;
		return 1;
	}
}

void abort_slow_workers(struct work_queue *q)
{
	struct work_queue_worker *w;
	char *key;
	const double multiplier = q->fast_abort_multiplier;

	if(q->total_tasks_complete < 10)
		return;

	timestamp_t average_task_time = q->total_task_time / q->total_tasks_complete;
	timestamp_t current = timestamp_get();

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->state == WORKER_STATE_BUSY) {
			timestamp_t runtime = current - w->current_task->transfer_start_time;
			if(runtime > (average_task_time * multiplier)) {
				debug(D_NOTICE, "%s (%s) has run too long: %.02lf s (average is %.02lf s)", w->hostname, w->addrport, runtime / 1000000.0, average_task_time / 1000000.0);
				remove_worker(q, w);
			}
		}
	}
}

int work_queue_port(struct work_queue *q)
{
	char addr[LINK_ADDRESS_MAX];
	int port;

	if(!q)
		return 0;

	if(link_address_local(q->master_link, addr, &port)) {
		return port;
	} else {
		return 0;
	}
}

struct work_queue *work_queue_create(int port)
{
	struct work_queue *q = malloc(sizeof(*q));
	char *envstring;

	memset(q, 0, sizeof(*q));

	if(port == 0) {
		envstring = getenv("WORK_QUEUE_PORT");
		if(envstring) {
			port = atoi(envstring);
		} else {
			// indicate using a random available port
			port = -1;
		}
	}

	if(port == WORK_QUEUE_RANDOM_PORT) {
		int lowport = 9000;
		int highport = 32767;

		envstring = getenv("WORK_QUEUE_LOW_PORT");
		if(envstring)
			lowport = atoi(envstring);

		envstring = getenv("WORK_QUEUE_HIGH_PORT");
		if(envstring)
			highport = atoi(envstring);

		for(port = lowport; port < highport; port++) {
			q->master_link = link_serve(port);
			if(q->master_link)
				break;
		}
	} else {
		q->master_link = link_serve(port);
	}

	if(!q->master_link)
		goto failure;

	q->ready_list = list_create();
	q->complete_list = list_create();
	q->worker_table = hash_table_create(0, 0);

	// The poll table is initially null, and will be created
	// (and resized) as needed by build_poll_table.
	q->poll_table_size = 8;
	q->poll_table = 0;

	int i;
	for(i = 0; i < WORKER_STATE_MAX; i++) {
		q->workers_in_state[i] = 0;
	}

	q->fast_abort_multiplier = wq_option_fast_abort_multiplier;
	q->worker_selection_algorithm = wq_option_scheduler;

	envstring = getenv("WORK_QUEUE_NAME");
	if(envstring)
		work_queue_specify_name(q, envstring);

	envstring = getenv("WORK_QUEUE_MASTER_MODE");
	if(envstring) {
		work_queue_specify_master_mode(q, atoi(envstring));
	} else {
		q->master_mode = WORK_QUEUE_MASTER_MODE_STANDALONE;
	}

	envstring = getenv("WORK_QUEUE_PRIORITY");
	if(envstring) {
		work_queue_specify_priority(q, atoi(envstring));
	} else {
		q->priority = WORK_QUEUE_MASTER_PRIORITY_DEFAULT;
	}

	envstring = getenv("WORK_QUEUE_WORKER_MODE");
	if(envstring) {
		work_queue_specify_worker_mode(q, atoi(envstring));
	} else {
		q->worker_mode = WORK_QUEUE_WORKER_MODE_SHARED;
	}

	if(q->master_mode == WORK_QUEUE_MASTER_MODE_CATALOG) {
		if(update_catalog(q)) {
			catalog_update_time = time(0);
		} else {
			fprintf(stderr, "Reporting master info to catalog server failed!");
		}
	}

	debug(D_WQ, "Work Queue is listening on port %d.", port);
	return q;

      failure:
	debug(D_NOTICE, "Could not create work_queue on port %i.", port);
	free(q);
	return 0;
}

int work_queue_specify_name(struct work_queue *q, const char *name)
{
	if(q && name) {
		if(q->name)
			free(q->name);
		q->name = strdup(name);
		setenv("WORK_QUEUE_NAME", q->name, 1);
	}

	return 0;
}

const char *work_queue_name(struct work_queue *q)
{
	return q->name;
}

int work_queue_specify_priority(struct work_queue *q, int priority)
{
	if(priority > 0 && priority <= WORK_QUEUE_MASTER_PRIORITY_MAX) {
		q->priority = priority;
	} else {
		q->priority = WORK_QUEUE_MASTER_PRIORITY_DEFAULT;
	}
	return q->priority;
}

int work_queue_specify_master_mode(struct work_queue *q, int mode)
{
	switch (mode) {
	case WORK_QUEUE_MASTER_MODE_CATALOG:
	case WORK_QUEUE_MASTER_MODE_STANDALONE:
		q->master_mode = mode;
		break;
	default:
		q->master_mode = WORK_QUEUE_MASTER_MODE_STANDALONE;
	}

	return q->master_mode;
}

int work_queue_specify_worker_mode(struct work_queue *q, int mode)
{
	switch (mode) {
	case WORK_QUEUE_WORKER_MODE_EXCLUSIVE:
	case WORK_QUEUE_WORKER_MODE_SHARED:
		q->worker_mode = mode;
		break;
	default:
		q->worker_mode = WORK_QUEUE_WORKER_MODE_SHARED;
	}
	return q->worker_mode;
}

void work_queue_delete(struct work_queue *q)
{
	if(q) {
		struct work_queue_worker *w;
		char *key;

		hash_table_firstkey(q->worker_table);
		while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
			remove_worker(q, w);
		}
		hash_table_delete(q->worker_table);
		list_delete(q->ready_list);
		list_delete(q->complete_list);
		free(q->poll_table);
		link_close(q->master_link);
		free(q);
	}
}

void work_queue_submit(struct work_queue *q, struct work_queue_task *t)
{
	/* If the task has been used before, clear out accumlated state. */

	if(t->output) {
		free(t->output);
		t->output = 0;
	}
	if(t->host) {
		free(t->host);
		t->host = 0;
	}
	t->total_transfer_time = 0;
	t->result = WORK_QUEUE_RESULT_UNSET;

	/* Then, add it to the ready list and mark it as submitted. */

	list_push_tail(q->ready_list, t);
	t->submit_time = timestamp_get();
	q->total_tasks_submitted++;
}

struct work_queue_task *work_queue_wait(struct work_queue *q, int timeout)
{
	struct work_queue_task *t;
	int i;
	time_t stoptime;
	int result;

	if(timeout == WORK_QUEUE_WAITFORTASK) {
		stoptime = 0;
	} else {
		stoptime = time(0) + timeout;
	}


	while(1) {
		if(q->master_mode == WORK_QUEUE_MASTER_MODE_CATALOG && time(0) - catalog_update_time >= WORK_QUEUE_CATALOG_UPDATE_INTERVAL) {
			update_catalog(q);
			catalog_update_time = time(0);
		}

		t = list_pop_head(q->complete_list);
		if(t)
			return t;

		if(q->workers_in_state[WORKER_STATE_BUSY] == 0 && list_size(q->ready_list) == 0)
			break;

		start_tasks(q);

		int n = build_poll_table(q);

		// Wait no longer than the caller's patience.
		int msec;
		if(stoptime) {
			msec = MAX(0, (stoptime - time(0)) * 1000);
		} else {
			msec = 5000;
		}

		result = link_poll(q->poll_table, n, msec);

		// If a process is waiting to complete, return without a task.
		if(process_pending())
			return 0;

		// If nothing was awake, restart the loop or return without a task.
		if(result <= 0) {
			if(stoptime && time(0) >= stoptime) {
				return 0;
			} else {
				continue;
			}
		}
		// If the master link was awake, then accept as many workers as possible.
		if(q->poll_table[0].revents) {
			do {
				add_worker(q);
			} while(link_usleep(q->master_link, 0, 1, 0));
		}
		// Then consider all existing active workers and dispatch tasks.
		for(i = 1; i < n; i++) {
			if(q->poll_table[i].revents) {
				handle_worker(q, q->poll_table[i].link);
			}
		}

		// If fast abort is enabled, kill off slow workers.
		if(q->fast_abort_multiplier > 0) {
			abort_slow_workers(q);
		}
	}

	return 0;
}


int work_queue_hungry(struct work_queue *q)
{
	struct work_queue_stats qs;
	int i, j;
	work_queue_get_stats(q, &qs);

	if(qs.total_tasks_dispatched < 100)
		return (100 - qs.total_tasks_dispatched);

	//i = 1.1 * number of current workers
	//j = # of queued tasks.
	//i-j = # of tasks to queue to re-reach the status quo.
	i = (1.1 * (qs.workers_init + qs.workers_ready + qs.workers_busy));
	j = (qs.tasks_waiting);
	return MAX(i - j, 0);
}


void work_queue_task_specify_tag(struct work_queue_task *t, const char *tag)
{
	if(t->tag)
		free(t->tag);
	t->tag = xstrdup(tag);
}

void work_queue_task_specify_preferred_host(struct work_queue_task *t, const char *hostname)
{
	if(t->preferred_host)
		free(t->preferred_host);
	t->preferred_host = xstrdup(hostname);
}

void work_queue_task_specify_file(struct work_queue_task *t, const char *local_name, const char *remote_name, int type, int flags)
{
	struct work_queue_file *tf = malloc(sizeof(struct work_queue_file));

	tf->type = WORK_QUEUE_FILE;
	tf->flags = flags;
	tf->length = strlen(local_name);
	tf->payload = strdup(local_name);
	tf->remote_name = strdup(remote_name);

	if(type == WORK_QUEUE_INPUT) {
		list_push_tail(t->input_files, tf);
	} else {
		list_push_tail(t->output_files, tf);
	}
}

void work_queue_task_specify_buffer(struct work_queue_task *t, const char *data, int length, const char *remote_name, int flags)
{
	struct work_queue_file *tf = malloc(sizeof(struct work_queue_file));
	tf->type = WORK_QUEUE_BUFFER;
	tf->flags = flags;
	tf->length = length;
	tf->payload = malloc(length);
	memcpy(tf->payload, data, length);
	tf->remote_name = strdup(remote_name);
	list_push_tail(t->input_files, tf);
}

void work_queue_task_specify_file_command(struct work_queue_task *t, const char *remote_name, const char *cmd, int type, int flags)
{
	struct work_queue_file *tf = malloc(sizeof(struct work_queue_file));
	tf->type = WORK_QUEUE_REMOTECMD;
	tf->flags = flags;
	tf->length = strlen(cmd);
	tf->payload = strdup(cmd);
	tf->remote_name = strdup(remote_name);

	if(type == WORK_QUEUE_INPUT) {
		list_push_tail(t->input_files, tf);
	} else {
		list_push_tail(t->output_files, tf);
	}
}

void work_queue_task_specify_output_file(struct work_queue_task *t, const char *rname, const char *fname)
{
	return work_queue_task_specify_file(t, fname, rname, WORK_QUEUE_OUTPUT, WORK_QUEUE_CACHE);
}

void work_queue_task_specify_output_file_do_not_cache(struct work_queue_task *t, const char *rname, const char *fname)
{
	return work_queue_task_specify_file(t, fname, rname, WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE);
}

void work_queue_task_specify_input_buf(struct work_queue_task *t, const char *buf, int length, const char *rname)
{
	return work_queue_task_specify_buffer(t, buf, length, rname, WORK_QUEUE_NOCACHE);
}

void work_queue_task_specify_input_file(struct work_queue_task *t, const char *fname, const char *rname)
{
	return work_queue_task_specify_file(t, fname, rname, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE);
}

void work_queue_task_specify_input_file_do_not_cache(struct work_queue_task *t, const char *fname, const char *rname)
{
	return work_queue_task_specify_file(t, fname, rname, WORK_QUEUE_INPUT, WORK_QUEUE_NOCACHE);
}

int work_queue_task_specify_algorithm(struct work_queue_task *t, int alg)
{
	if(t && alg >= WORK_QUEUE_SCHEDULE_UNSET && alg <= WORK_QUEUE_SCHEDULE_MAX) {
		t->worker_selection_algorithm = alg;
		return 0;
	} else {
		return 1;
	}
}

int work_queue_specify_algorithm(struct work_queue *q, int alg)
{
	if(q && alg > WORK_QUEUE_SCHEDULE_UNSET && alg <= WORK_QUEUE_SCHEDULE_MAX) {
		q->worker_selection_algorithm = alg;
		return 0;
	} else {
		return 1;
	}
}

int work_queue_shut_down_workers(struct work_queue *q, int n)
{
	struct work_queue_worker *w;
	char *key;
	int i = 0;

	if(!q)
		return -1;

	// send worker exit.
	hash_table_firstkey(q->worker_table);
	while((n == 0 || i < n) && hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		link_putliteral(w->link, "exit\n", time(0) + short_timeout);
		remove_worker(q, w);
		i++;
	}

	return i;
}

int work_queue_empty(struct work_queue *q)
{
	return ((list_size(q->ready_list) + list_size(q->complete_list) + q->workers_in_state[WORKER_STATE_BUSY]) == 0);
}

static int tolerable_transfer_rate_denominator = 10;
static long double minimum_allowed_transfer_rate = 100000;	// 100 KB/s
static timestamp_t get_transfer_wait_time(struct work_queue *q, struct work_queue_worker *w, INT64_T length)
{
	timestamp_t timeout;
	struct work_queue_task *t;
	long double avg_queue_transfer_rate, avg_worker_transfer_rate, retry_transfer_rate, tolerable_transfer_rate;
	INT64_T total_tasks_complete, total_tasks_running, total_tasks_waiting, num_of_free_workers;

	t = w->current_task;

	if(w->total_transfer_time) {
		avg_worker_transfer_rate = (long double) w->total_bytes_transferred / w->total_transfer_time * 1000000;
	} else {
		avg_worker_transfer_rate = 0;
	}

	retry_transfer_rate = 0;
	num_of_free_workers = q->workers_in_state[WORKER_STATE_INIT] + q->workers_in_state[WORKER_STATE_READY];
	total_tasks_complete = q->total_tasks_complete;
	total_tasks_running = q->workers_in_state[WORKER_STATE_BUSY];
	total_tasks_waiting = list_size(q->ready_list);
	if(total_tasks_complete > total_tasks_running && num_of_free_workers > total_tasks_waiting) {
		// The master has already tried most of the workers connected and has free workers for retrying slow workers
		if(t->total_bytes_transferred) {
			avg_queue_transfer_rate = (long double) (q->total_bytes_sent + q->total_bytes_received) / (q->total_send_time + q->total_receive_time) * 1000000;
			retry_transfer_rate = (long double) length / t->total_bytes_transferred * avg_queue_transfer_rate;
		}
	}

	tolerable_transfer_rate = MAX(avg_worker_transfer_rate / tolerable_transfer_rate_denominator, retry_transfer_rate);
	tolerable_transfer_rate = MAX(minimum_allowed_transfer_rate, tolerable_transfer_rate);

	timeout = MAX(2, length / tolerable_transfer_rate);	// try at least 2 seconds

	debug(D_WQ, "%s (%s) will try up to %lld seconds for the transfer of this %.3Lf MB file.", w->hostname, w->addrport, timeout, (long double) length / 1000000);
	return timeout;
}

static int update_catalog(struct work_queue *q)
{
	char address[DATAGRAM_ADDRESS_MAX];
	char owner[USERNAME_MAX];
	static char text[WORK_QUEUE_CATALOG_LINE_MAX];
	struct work_queue_stats s;
	int port;

	if(!outgoing_datagram) {
		outgoing_datagram = datagram_create(0);
		if(!outgoing_datagram) {
			fprintf(stderr, "Couldn't create outgoing udp port, thus work queue master info won't be sent to the catalog server!");
			return 0;
		}
	}

	port = work_queue_port(q);
	work_queue_get_stats(q, &s);

	if(!username_get(owner)) {
		strcpy(owner,"unknown");
	}

	snprintf(text, WORK_QUEUE_CATALOG_LINE_MAX,
		 "type wq_master\nproject %s\npriority %d\nport %d\nlifetime %d\ntasks_waiting %d\ntasks_complete %d\ntask_running%d\ntotal_tasks_dispatched %d\nworkers_init %d\nworkers_ready %d\nworkers_busy %d\nworkers %d\nversion %d.%d.%d\nowner %s", q->name, q->priority, port,
		 WORK_QUEUE_CATALOG_LIFETIME, s.tasks_waiting, s.total_tasks_complete, s.tasks_running, s.total_tasks_dispatched, s.workers_init, s.workers_ready, s.workers_busy, s.workers_init + s.workers_ready + s.workers_busy, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, owner);

	if(domain_name_cache_lookup(CATALOG_HOST, address)) {
		debug(D_WQ, "sending master information to %s:%d", CATALOG_HOST, CATALOG_PORT);
		datagram_send(outgoing_datagram, text, strlen(text), address, CATALOG_PORT);
	}

	return 1;
}
