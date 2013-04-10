/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"
#include "work_queue_protocol.h"

#include "cctools.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "work_queue_catalog.h"
#include "datagram.h"
#include "domain_name_cache.h"
#include "nvpair.h"
#include "copy_stream.h"
#include "memory_info.h"
#include "disk_info.h"
#include "hash_cache.h"
#include "link.h"
#include "list.h"
#include "xxmalloc.h"
#include "debug.h"
#include "stringtools.h"
#include "load_average.h"
#include "domain_name_cache.h"
#include "getopt.h"
#include "full_io.h"
#include "create_dir.h"
#include "delete_dir.h"
#include "macros.h"

#include <unistd.h>

#include <dirent.h>
#include <fcntl.h>

#include <sys/mman.h>
#include <sys/poll.h>
#include <sys/resource.h>
#include <sys/signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/wait.h>

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef CCTOOLS_OPSYS_SUNOS
extern int setenv(const char *name, const char *value, int overwrite);
#endif

#define STDOUT_BUFFER_SIZE 1048576        

#define PIPE_ACTIVE 1
#define LINK_ACTIVE 2
#define POLL_FAIL 4

#define TASK_NONE 0
#define TASK_RUNNING 1

struct task_info {
	pid_t pid;
	int status;
	struct rusage rusage;
	timestamp_t execution_end;
	char *output;
	INT64_T output_length;
};

struct distribution_node {
	void *item;
	int weight;
};

// Maximum time to wait before aborting if there is no connection to the master.
static int idle_timeout = 900;

// Maximum time to wait before switching to another master. Used in auto mode.
static const int master_timeout = 15;

// Maximum time to wait when actively communicating with the master.
static const int active_timeout = 3600;

// The timeout in which a bad master expires
static const int bad_master_expiration_timeout = 15;

// A short timeout constant
static const int short_timeout = 5;

// Initial value for backoff interval (in seconds) when worker fails to connect to a master.
static int init_backoff_interval = 1; 

// Maximum value for backoff interval (in seconds) when worker fails to connect to a master.
static int max_backoff_interval = 60; 

// Flag gets set on receipt of a terminal signal.
static int abort_flag = 0;

//Threshold for available disk space (MB) beyond which clean up and restart.
static UINT64_T disk_avail_threshold = 100;


// Basic worker global variables
static char actual_addr[LINK_ADDRESS_MAX];
static int actual_port;
static char workspace[WORKER_WORKSPACE_NAME_MAX];
static char *os_name = NULL; 
static char *arch_name = NULL;
static char *user_specified_workdir = NULL;

// Forked task related
static int pipefds[2];
static int task_status= TASK_NONE;
static timestamp_t execution_start;
static pid_t pid;
static void *stdout_buffer = NULL;		// a buffer that holds the stdout of a task (read from the pipe)
static size_t stdout_buffer_used = 0;
static char stdout_file[50];			// name of the file that stores a task's stdout 
static int stdout_file_fd = 0;
static int stdout_in_file = 0;

// Catalog mode control variables
static char *catalog_server_host = NULL;
static int catalog_server_port = 0;
static int auto_worker = 0;
static char *pool_name = NULL;
static struct work_queue_master *actual_master = NULL;
static struct list *preferred_masters = NULL;
static struct hash_cache *bad_masters = NULL;
static int released_by_master = 0;

static void report_worker_ready(struct link *master)
{
	char hostname[DOMAIN_NAME_MAX];
	UINT64_T memory_avail, memory_total;
	UINT64_T disk_avail, disk_total;
	int ncpus;
	char *name_of_master;
	char *name_of_pool;

	domain_name_cache_guess(hostname);
	ncpus = load_average_get_cpus();
	memory_info_get(&memory_avail, &memory_total);
	disk_info_get(".", &disk_avail, &disk_total);
	name_of_master = actual_master ? actual_master->proj : WORK_QUEUE_PROTOCOL_BLANK_FIELD;
	name_of_pool = pool_name ? pool_name : WORK_QUEUE_PROTOCOL_BLANK_FIELD;

	link_putfstring(master, "ready %s %d %llu %llu %llu %llu %s %s %s %s %s\n", time(0) + active_timeout, hostname, ncpus, memory_avail, memory_total, disk_avail, disk_total, name_of_master, name_of_pool, os_name, arch_name, workspace);
}


static void clear_task_info(struct task_info *ti)
{
	if(ti->output) {
		free(ti->output);
	}
	memset(ti, 0, sizeof(*ti));
}

static pid_t execute_task(const char *cmd)
{
	pid_t pid;

	// reset the internal stdout buffer
	stdout_buffer_used = 0;

	fflush(NULL);

	if(pipe(pipefds) == -1) {
		debug(D_WQ, "Failed to create pipe for output redirecting: %s.", strerror(errno));
		return 0;
	}

	pid = fork();
	if(pid > 0) {
		// Set pipe's read end to be non-blocking
		int flag;
		flag = fcntl(pipefds[0], F_GETFL);
		flag |= O_NONBLOCK;	
		fcntl(pipefds[0], F_SETFL, flag);

		// Close pipe's write end
		close(pipefds[1]);
	
		// Make child process the leader of its own process group. This allows
		// signals to also be delivered to processes forked by the child process.
		// This is currently used by kill_task(). 
		setpgid(pid, 0); 
		
		debug(D_WQ, "started process %d: %s", pid, cmd);
		return pid;
	} else if(pid < 0) {
		debug(D_WQ, "couldn't create new process: %s\n", strerror(errno));
		close(pipefds[0]);
		close(pipefds[1]);
		return pid;
	} else {
		int fd = open("/dev/null", O_RDONLY);
		if (fd == -1) fatal("could not open /dev/null: %s", strerror(errno));
		int result = dup2(fd, STDIN_FILENO);
		if (result == -1) fatal("could not dup /dev/null to stdin: %s", strerror(errno));

		result = dup2(pipefds[1], STDOUT_FILENO);
		if (result == -1) fatal("could not dup pipe to stdout: %s", strerror(errno));

		result = dup2(pipefds[1], STDERR_FILENO);
		if (result == -1) fatal("could not dup pipe to stderr: %s", strerror(errno));

		close(pipefds[0]);
		close(pipefds[1]);

		execlp("sh", "sh", "-c", cmd, (char *) 0);
		_exit(127);	// Failed to execute the cmd.
	}
	return 0;
}

static int errno_is_temporary(int e)
{
	if(e == EINTR || e == EWOULDBLOCK || e == EAGAIN || e == EINPROGRESS || e == EALREADY || e == EISCONN) {
		return 1;
	} else {
		return 0;
	}
}

static int read_task_stdout(time_t stoptime) {
	ssize_t chunk;
	char *buffer;
	size_t space_left;

	while (1) {
		buffer = stdout_buffer + stdout_buffer_used;
		space_left = STDOUT_BUFFER_SIZE - stdout_buffer_used;

		if(space_left > 0) {
			chunk = read(pipefds[0], buffer, space_left);
			if(chunk < 0) {
				if(errno_is_temporary(errno)) {
					return 0;
				} else {
					return -1;
				}
			} else if (chunk == 0) {
				// task done
				close(pipefds[0]);
				return 1;
			} else {
				stdout_buffer_used += chunk;
			}
		} else {
			if(full_write(stdout_file_fd, stdout_buffer, stdout_buffer_used) == -1) {
				return -1;
			}
			stdout_in_file = 1;
			stdout_buffer_used = 0;

			if(stoptime < time(0)) {
				return 0;
			}
		}
	}
}

static void get_task_stdout(char **task_output, INT64_T *task_output_length) {
    // flush stdout buffer to stdout file if needed
    if(stdout_in_file) {
	if(full_write(stdout_file_fd, stdout_buffer, stdout_buffer_used) == -1) {
	    debug(D_WQ, "Task stdout truncated: failed to write contents to file - %s.\n", stdout_file);
	}
    }
    close(stdout_file_fd);

    // Record stdout of the child process
    char *output;
    INT64_T length;
    if(stdout_in_file) { 
	// Task stdout is in a file on disk. Extract the file contents into a buffer.
	FILE *stream = fopen(stdout_file, "r");
	if(stream) {
	    length = copy_stream_to_buffer(stream, &output);
	    if(length <= 0) {
		length = 0;
		if(output) {
		    free(output);
		}
		output = NULL;
	    }
	    fclose(stream);
	} else {
	    debug(D_WQ, "Couldn't open the file that stores the standard output: %s\n", strerror(errno));
	    length = 0;
	    output = NULL;
	}
	unlink(stdout_file);
	stdout_in_file = 0;
    } else { 
	// Task stdout is all in a buffer
	length = stdout_buffer_used;
	output = malloc(length);
	if(output) {
	    memcpy(output, stdout_buffer, length); 
	} else {
	    length = 0;
	}
    }

    *task_output = output;
    *task_output_length = length;
}


// Wait for the task process to terminate and collect information about the
// task process if parameter 'ti' is not NULL
static int wait_task_process(struct task_info *ti) {
	pid_t tmp_pid;
	int status;
	struct rusage rusage;
	int flags = 0;

	tmp_pid = wait4(pid, &status, flags, &rusage);

	if(tmp_pid != pid) {
		return 0;
	} 
	
	task_status = TASK_NONE;

	char *output;
	INT64_T output_length;
	// Cleanup the file/buffer used to store task stdout 
	get_task_stdout(&output, &output_length);

	if(ti) {
		memset(ti, 0, sizeof(struct task_info));
		ti->pid = tmp_pid;
		ti->status = status;
		ti->rusage = rusage;
		ti->execution_end = timestamp_get();
		ti->output = output;
		ti->output_length = output_length;
	} else {
		free(output);
	}

	return 1;
}

static void report_task_complete(struct link *master, int result, char *output, INT64_T output_length, timestamp_t execution_time)
{
	debug(D_WQ, "Task complete: result %d %lld %llu", result, output_length, execution_time);
	link_putfstring(master, "result %d %lld %llu\n", time(0) + active_timeout, result, output_length, execution_time);
	link_putlstring(master, output, output_length, time(0) + active_timeout);
}

static int handle_task(struct link *master) {
	int result = read_task_stdout(time(0) + short_timeout);

	// Failed to retrieve task standard output
	if(result == -1) {
		return 0;
	}

	if(result == 1) {
		// Task has terminated.
		struct task_info ti;
		if(!wait_task_process(&ti)) {
			return 0;
		}

		if (!WIFEXITED(ti.status)){
			debug(D_WQ, "Task (process %d) did not exit normally.\n", pid);
		} 

		report_task_complete(master, ti.status, ti.output, ti.output_length, ti.execution_end - execution_start);
		clear_task_info(&ti);
	} 

	return 1;
}

static void make_hash_key(const char *addr, int port, char *key)
{
	sprintf(key, "%s:%d", addr, port);
}

static int check_disk_space_for_filesize(INT64_T file_size) {
    UINT64_T disk_avail, disk_total;

    // Check available disk space
    if(disk_avail_threshold > 0) {
	disk_info_get(".", &disk_avail, &disk_total);
	if(file_size > 0) {	
	    if((UINT64_T)file_size > disk_avail || (disk_avail - file_size) < disk_avail_threshold) {
		debug(D_WQ, "Incoming file of size %lld MB will lower available disk space (%llu MB) below threshold (%llu MB).\n", file_size/MEGA, disk_avail/MEGA, disk_avail_threshold/MEGA);
		return 0;
	    }
	} else {
	    if(disk_avail < disk_avail_threshold) {
		debug(D_WQ, "Available disk space (%llu MB) lower than threshold (%llu MB).\n", disk_avail/MEGA, disk_avail_threshold/MEGA);
		return 0;
	    }	
	}	
    }

    return 1;
}

/**
 * Reasons for a master being bad:
 * 1. The master does not need more workers right now;
 * 2. The master is already shut down but its record is still in the catalog server.
 */
static void record_bad_master(struct work_queue_master *m)
{
	char key[LINK_ADDRESS_MAX + 10];	// addr:port

	if(!m)
		return;

	make_hash_key(m->addr, m->port, key);
	hash_cache_insert(bad_masters, key, m, bad_master_expiration_timeout);
	debug(D_WQ, "Master at %s:%d is not receiving more workers.\nWon't connect to this master in %d seconds.", m->addr, m->port, bad_master_expiration_timeout);
}

static int reset_preferred_masters(struct work_queue_pool *pool) {
	char *pm;

	while((pm = (char *)list_pop_head(preferred_masters))) {
		free(pm);
	}

	int count = 0;

	char *pds = xxstrdup(pool->decision);
	char *pd = strtok(pds, " \t,"); 
	while(pd) {
		char *eq = strchr(pd, ':');
		if(eq) {
			*eq = '\0';
			if(list_push_tail(preferred_masters, xxstrdup(pd))) {
				count++;
			} else {
				fprintf(stderr, "Error: failed to insert item during resetting preferred masters.\n");
			}
			*eq = ':';
		} else {
			if(!strncmp(pd, "n/a", 4)) {
				break;
			} else {
				fprintf(stderr, "Invalid pool decision item: \"%s\".\n", pd);
				break;
			}
		}
		pd = strtok(0, " \t,");
	}
	free(pds);
	return count;
}

static int get_masters_and_pool_info(const char *catalog_host, int catalog_port, struct list **masters_list, struct work_queue_pool **pool)
{
	struct catalog_query *q;
	struct nvpair *nv;
	char *pm;
	struct work_queue_master *m;
	time_t timeout = 60, stoptime;
	char key[LINK_ADDRESS_MAX + 10];	// addr:port

	stoptime = time(0) + timeout;

	*pool = NULL;

	struct list *ml = list_create();
	if(!ml) {
		fprintf(stderr, "Failed to create list structure to store masters information.\n");
		*masters_list = NULL;
		return 0;
	}
	*masters_list = ml;

	int work_queue_pool_not_found;

	if(pool_name) {
		work_queue_pool_not_found = 1;
	} else {
		// pool name has not been set by -p option
		work_queue_pool_not_found = 0;
	}

	q = catalog_query_create(catalog_host, catalog_port, stoptime);
	if(!q) {
		fprintf(stderr, "Failed to query catalog server at %s:%d\n", catalog_host, catalog_port);
		return 0;
	}

	while((nv = catalog_query_read(q, stoptime))) {
		if(strcmp(nvpair_lookup_string(nv, "type"), CATALOG_TYPE_WORK_QUEUE_MASTER) == 0) {
			m = parse_work_queue_master_nvpair(nv);

			// exclude 'bad' masters
			make_hash_key(m->addr, m->port, key);
			if(!hash_cache_lookup(bad_masters, key)) {
				list_push_priority(ml, m, m->priority);
			}
		} 
		if(work_queue_pool_not_found) {
			if(strcmp(nvpair_lookup_string(nv, "type"), CATALOG_TYPE_WORK_QUEUE_POOL) == 0) {
				struct work_queue_pool *tmp_pool;
				tmp_pool = parse_work_queue_pool_nvpair(nv);
				if(strncmp(tmp_pool->name, pool_name, WORK_QUEUE_POOL_NAME_MAX) == 0) {
					*pool = tmp_pool;
					work_queue_pool_not_found = 0;
				} else {
					free_work_queue_pool(tmp_pool);
				}
			}
		}
		nvpair_delete(nv);
	}

	if(*pool) {
		reset_preferred_masters(*pool);
	}

	// trim masters list
	list_first_item(ml);
	while((m = (struct work_queue_master *)list_next_item(ml))) {
		list_first_item(preferred_masters);
		while((pm = (char *)list_next_item(preferred_masters))) {
			if(whole_string_match_regex(m->proj, pm)) {
				// preferred master found
				break;
			}
		}

		if(!pm) {
			// Master name does not match any of the preferred master names
			list_remove(ml, m);
			free_work_queue_master(m);
		}
	}

	// Must delete the query otherwise it would occupy 1 tcp connection forever!
	catalog_query_delete(q);
	return 1;
}
static void *select_item_by_weight(struct distribution_node distribution[], int n)
{
	// A distribution example:
	// struct distribution_node array[3] = { {"A", 20}, {"B", 30}, {"C", 50} };
	int i, w, x, sum;

	sum = 0;
	for(i = 0; i < n; i++) {
		w = distribution[i].weight;
		if(w < 0)
			return NULL;
		sum += w;
	}

	if(sum == 0) {
		return NULL;
	}

	x = rand() % sum;

	for(i = 0; i < n; i++) {
		x -= distribution[i].weight;
		if(x <= 0)
			return distribution[i].item;
	}

	return NULL;
}

static struct work_queue_master * select_master(struct list *ml, struct work_queue_pool *pool) {
	if(!ml) {
		return NULL;
	}

	if(!list_size(ml)) {
		return NULL;
	}

	if(!pool) {
		return list_pop_head(ml);
	}

	struct distribution_node *distribution;
	distribution = xxmalloc(sizeof(struct distribution_node)*list_size(ml));

	debug(D_WQ, "Selecting a project from %d project(s).", list_size(ml));
    struct work_queue_master *m;
	int i = 0;
	list_first_item(ml);
	while((m = (struct work_queue_master *)list_next_item(ml))) {
		(distribution[i]).item = (void *)m;
		int provided = workers_by_item(m->workers_by_pool, pool->name);
		int target = workers_by_item(pool->decision, m->proj);
		if(provided == -1) {
			provided = 0;
		}
		int n = target - provided;
		if(n < 0) {
			n = 0;
		}
		(distribution[i]).weight = n;
		debug(D_WQ, "\tproject: %s; weight: %d", m->proj, n);
		i++;
	}

   	m = (struct work_queue_master *)select_item_by_weight(distribution, list_size(ml));
	debug(D_WQ, "Selected project: %s", m->proj);
	if(m) {
		list_remove(ml, m);
	}
	free(distribution);
	return m;
}

static struct link *auto_link_connect(char *addr, int *port)
{
	struct link *master = NULL;
	struct list *ml;
	struct work_queue_master *m;
	struct work_queue_pool *pool;

	get_masters_and_pool_info(catalog_server_host, catalog_server_port, &ml, &pool);
	if(!ml) {
		return NULL;
	}
	debug_print_masters(ml);
	
	while((m = (struct work_queue_master *)select_master(ml, pool))) {
		master = link_connect(m->addr, m->port, time(0) + master_timeout);
		if(master) {
			debug(D_WQ, "talking to the master at:\n");
			debug(D_WQ, "addr:\t%s\n", m->addr);
			debug(D_WQ, "port:\t%d\n", m->port);
			debug(D_WQ, "project:\t%s\n", m->proj);
			debug(D_WQ, "priority:\t%d\n", m->priority);
			debug(D_WQ, "\n");

			strncpy(addr, m->addr, LINK_ADDRESS_MAX);
			(*port) = m->port;

			if(actual_master) {
				free_work_queue_master(actual_master);
			}
			actual_master = duplicate_work_queue_master(m);

			break;
		} else {
			record_bad_master(duplicate_work_queue_master(m));
		}
	}

	free_work_queue_master_list(ml);
	free_work_queue_pool(pool);

	return master;
}

/**
 * Stream file/directory contents for the rget protocol.
 * Format:
 * 		for a directory: a new line in the format of "dir $DIR_NAME 0"
 * 		for a file: a new line in the format of "file $FILE_NAME $FILE_LENGTH"
 * 					then file contents.
 * 		string "end" at the end of the stream (on a new line).
 *
 * Example:
 * Assume we have the following directory structure:
 * mydir
 * 		-- 1.txt
 * 		-- 2.txt
 * 		-- mysubdir
 * 			-- a.txt
 * 			-- b.txt
 * 		-- z.jpg
 *
 * The stream contents would be:
 *
 * dir mydir 0
 * file 1.txt $file_len
 * $$ FILE 1.txt's CONTENTS $$
 * file 2.txt $file_len
 * $$ FILE 2.txt's CONTENTS $$
 * dir mysubdir 0
 * file mysubdir/a.txt $file_len
 * $$ FILE mysubdir/a.txt's CONTENTS $$
 * file mysubdir/b.txt $file_len
 * $$ FILE mysubdir/b.txt's CONTENTS $$
 * file z.jpg $file_len
 * $$ FILE z.jpg's CONTENTS $$
 * end
 *
 */
static int stream_output_item(struct link *master, const char *filename)
{
	DIR *dir;
	struct dirent *dent;
	char dentline[WORK_QUEUE_LINE_MAX];
	struct stat info;
	INT64_T actual, length;
	int fd;

	if(stat(filename, &info) != 0) {
		goto failure;
	}

	if(S_ISDIR(info.st_mode)) {
		// stream a directory
		dir = opendir(filename);
		if(!dir) {
			goto failure;
		}
		link_putfstring(master, "dir %s %lld\n", time(0) + active_timeout, filename, (INT64_T) 0);

		while((dent = readdir(dir))) {
			if(!strcmp(dent->d_name, ".") || !strcmp(dent->d_name, ".."))
				continue;
			sprintf(dentline, "%s/%s", filename, dent->d_name);
			stream_output_item(master, dentline);
		}

		closedir(dir);
	} else {
		// stream a file
		fd = open(filename, O_RDONLY, 0);
		if(fd >= 0) {
			length = (INT64_T) info.st_size;
			link_putfstring(master, "file %s %lld\n", time(0) + active_timeout, filename, length);
			actual = link_stream_from_fd(master, fd, length, time(0) + active_timeout);
			close(fd);
			if(actual != length) {
				debug(D_WQ, "Sending back output file - %s failed: bytes to send = %lld and bytes actually sent = %lld.", filename, length, actual);
				return 0;
			}
		} else {
			goto failure;
		}
	}

	return 1;

failure:
	fprintf(stderr, "Failed to transfer ouput item - %s. (%s)\n", filename, strerror(errno));
	link_putfstring(master, "missing %s %d\n", time(0) + active_timeout, filename, errno);
	return 0;
}

static struct link *connect_master(time_t stoptime) {
	struct link *master = NULL;
	int backoff_multiplier = 2; 
	int backoff_interval = init_backoff_interval;

	while(!abort_flag) {
		if(stoptime < time(0)) {
			// Failed to connect to any master.
			if(auto_worker) {
				debug(D_NOTICE, "work_queue_worker: giving up because couldn't connect to any master in %d seconds.\n", idle_timeout);
			} else {
				debug(D_NOTICE, "work_queue_worker: giving up because couldn't connect to %s:%d in %d seconds.\n", actual_addr, actual_port, idle_timeout);
			}
			break;
		}

		if(auto_worker) {
			master = auto_link_connect(actual_addr, &actual_port);
		} else {
			master = link_connect(actual_addr, actual_port, time(0) + master_timeout);
		}

		if(!master) {
			if (backoff_interval > max_backoff_interval) {
				backoff_interval = max_backoff_interval;
			}
		
			sleep(backoff_interval);
			backoff_interval *= backoff_multiplier;
			continue;
		}

		link_tune(master, LINK_TUNE_INTERACTIVE);
		report_worker_ready(master);

		//reset backoff interval after connection to master.
		backoff_interval = init_backoff_interval; 

		return master;
	}

	return NULL;
}


static int poll_master_and_task(struct link *master, int task_pipe_fd, int timeout)
{
	struct pollfd pfds[2];
	int result, ret, n;

	pfds[0].fd = link_fd(master);
	pfds[0].events = POLLIN;
	pfds[0].revents = 0;

	pfds[1].fd = task_pipe_fd;
	pfds[1].events = POLLIN;
	pfds[1].revents = 0;

	ret = 0;

	if(task_status == TASK_RUNNING) {
		n = 2;
	} else {
		n = 1;
	}

	int msec = timeout * 1000;
	// check if master link has buffer contents.
	if(!link_buffer_empty(master)) {
		ret |= LINK_ACTIVE;
		msec = 0;
	}

	result = poll(pfds, n, msec);
	if (result > 0) {
		if (pfds[0].revents & POLLIN) {
			ret |= LINK_ACTIVE;
		}

		if (pfds[0].revents & POLLHUP) {
			ret |= LINK_ACTIVE;
		}

		if(n == 2) {
			if (pfds[1].revents & POLLIN) {
				ret |= PIPE_ACTIVE;
			}

			if (pfds[1].revents & POLLHUP) {
				ret |= PIPE_ACTIVE;
			}
		}
	} else if (result == 0) {
	} else {
		if (!errno_is_temporary(errno)) {
			// errno is not temporary, something is broken then
			ret |= POLL_FAIL;
		}
	} 

	return ret;
}

static int do_work(struct link *master, INT64_T length) {
	char *cmd = malloc(length + 10);

	link_read(master, cmd, length, time(0) + active_timeout);
	cmd[length] = 0;

	debug(D_WQ, "%s", cmd);
	execution_start = timestamp_get();

	pid = execute_task(cmd);
	free(cmd);
	if(pid < 0) {
		fprintf(stderr, "work_queue_worker: failed to fork task. Shutting down worker...\n");
		abort_flag = 1;
		return 0;
	}

	snprintf(stdout_file, 50, "%d.task.stdout.tmp", pid);
	if((stdout_file_fd = open(stdout_file, O_CREAT | O_WRONLY)) == -1) {
		fprintf(stderr, "work_queue_worker: failed to open standard output file. Shutting down worker...\n");
		abort_flag = 1;
		return 0;
	}

	task_status = TASK_RUNNING;
	return 1;
}

static int do_stat(struct link *master, const char *filename) {
	struct stat st;
	if(!stat(filename, &st)) {
		debug(D_WQ, "result 1 %lu %lu", (unsigned long int) st.st_size, (unsigned long int) st.st_mtime);
		link_putfstring(master, "result 1 %lu %lu\n", time(0) + active_timeout, (unsigned long int) st.st_size, (unsigned long int) st.st_mtime);
	} else {
		debug(D_WQ, "result 0 0 0");
		link_putliteral(master, "result 0 0 0\n", time(0) + active_timeout);
	}
	return 1;
}

static int do_symlink(const char *path, char *filename) {
	int mode = 0;
	char *cur_pos, *tmp_pos;

	cur_pos = filename;

	if(!strncmp(cur_pos, "./", 2)) {
		cur_pos += 2;
	}

	tmp_pos = strrchr(cur_pos, '/');
	if(tmp_pos) {
		*tmp_pos = '\0';
		if(!create_dir(cur_pos, mode | 0700)) {
			debug(D_WQ, "Could not create directory - %s (%s)\n", cur_pos, strerror(errno));
			return 0;
		}
		*tmp_pos = '/';
	}
	symlink(path, filename);
	return 1;
}

static int do_put(struct link *master, char *filename, INT64_T length, int mode) {

	if(!check_disk_space_for_filesize(length)) {
		return 0;
	}

	mode = mode | 0600;
	char *cur_pos, *tmp_pos;

	cur_pos = filename;

	if(!strncmp(cur_pos, "./", 2)) {
		cur_pos += 2;
	}

	tmp_pos = strrchr(cur_pos, '/');
	if(tmp_pos) {
		*tmp_pos = '\0';
		if(!create_dir(cur_pos, mode | 0700)) {
			debug(D_WQ, "Could not create directory - %s (%s)\n", cur_pos, strerror(errno));
			return 0;
		}
		*tmp_pos = '/';
	}

	int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, mode);
	if(fd < 0) {
		return 0;
	}

	INT64_T actual = link_stream_to_fd(master, fd, length, time(0) + active_timeout);
	close(fd);
	if(actual != length) {
		return 0;
	}

	return 1;
}

static int do_unlink(const char *path) {
	//Use delete_dir() since it calls unlink() if path is a file.	
	if(delete_dir(path) != 0) { 
		struct stat buf;
		if(stat(path, &buf) != 0) {
			if(errno == ENOENT) {
				// If the path does not exist, return success
				return 1;
			}
		}
		// Failed to do unlink
		return 0;
	}	
	return 1;
}

static int do_mkdir(const char *filename, int mode) {
	if(!create_dir(filename, mode | 0700)) {
		debug(D_WQ, "Could not create directory - %s (%s)\n", filename, strerror(errno));
		return 0;
	}
	return 1;
}

static int do_rget(struct link *master, const char *filename) {
	stream_output_item(master, filename);
	link_putliteral(master, "end\n", time(0) + active_timeout);
	return 1;
}

static int do_get(struct link *master, const char *filename) {
	int fd;
	INT64_T length;
	struct stat info;
	if(stat(filename, &info) != 0) {
		fprintf(stderr, "Output file %s was not created. (%s)\n", filename, strerror(errno));
		return 0;
	}
	// send back a single file
	fd = open(filename, O_RDONLY, 0);
	if(fd >= 0) {
		length = (INT64_T) info.st_size;
		link_putfstring(master, "%lld\n", time(0) + active_timeout, length);
		INT64_T actual = link_stream_from_fd(master, fd, length, time(0) + active_timeout);
		close(fd);
		if(actual != length) {
			debug(D_WQ, "Sending back output file - %s failed: bytes to send = %lld and bytes actually sent = %lld.\nEntering recovery process now ...\n", filename, length, actual);
			return 0;
		}
	} else {
		fprintf(stderr, "Could not open output file %s. (%s)\n", filename, strerror(errno));
		return 0;
	}

	return 1;
}

static int do_thirdget(int mode, char *filename, const char *path) {
	char cmd[WORK_QUEUE_LINE_MAX];
	char *cur_pos, *tmp_pos;

	if(mode != WORK_QUEUE_FS_CMD) {
		struct stat info;
		if(stat(path, &info) != 0) {
			debug(D_WQ, "Path %s not accessible. (%s)\n", path, strerror(errno));
			return 0;
		}
		if(!strcmp(filename, path)) {
			debug(D_WQ, "thirdget aborted: filename (%s) and path (%s) are the same\n", filename, path);
			return 1;
		}
	}

	cur_pos = filename;
	if(!strncmp(cur_pos, "./", 2)) {
		cur_pos += 2;
	}

	tmp_pos = strrchr(cur_pos, '/');
	if(tmp_pos) {
		*tmp_pos = '\0';
		if(!create_dir(cur_pos, mode | 0700)) {
			debug(D_WQ, "Could not create directory - %s (%s)\n", cur_pos, strerror(errno));
			return 0;
		}
		*tmp_pos = '/';
	}

	switch (mode) {
	case WORK_QUEUE_FS_SYMLINK:
		if(symlink(path, filename) != 0) {
			debug(D_WQ, "Could not thirdget %s, symlink (%s) failed. (%s)\n", filename, path, strerror(errno));
			return 0;
		}
	case WORK_QUEUE_FS_PATH:
		sprintf(cmd, "/bin/cp %s %s", path, filename);
		if(system(cmd) != 0) {
			debug(D_WQ, "Could not thirdget %s, copy (%s) failed. (/bin/cp %s)\n", filename, path, filename, strerror(errno));
			return 0;
		}
		break;
	case WORK_QUEUE_FS_CMD:
		if(system(path) != 0) {
			debug(D_WQ, "Could not thirdget %s, command (%s) failed. (%s)\n", filename, path, strerror(errno));
			return 0;
		}
		break;
	}
	return 1;
}

static int do_thirdput(struct link *master, int mode, const char *filename, const char *path) {
	struct stat info;
	char cmd[WORK_QUEUE_LINE_MAX];

	switch (mode) {
	case WORK_QUEUE_FS_SYMLINK:
	case WORK_QUEUE_FS_PATH:
		if(stat(filename, &info) != 0) {
			debug(D_WQ, "File %s not accessible. (%s)\n", filename, strerror(errno));
			return 0;
		}
		if(!strcmp(filename, path)) {
			debug(D_WQ, "thirdput aborted: filename (%s) and path (%s) are the same\n", filename, path);
			return 1;
		}
		sprintf(cmd, "/bin/cp %s %s", filename, path);
		if(system(cmd) != 0) {
			debug(D_WQ, "Could not thirdput %s, copy (%s) failed. (%s)\n", filename, path, strerror(errno));
			return 0;
		}
		break;
	case WORK_QUEUE_FS_CMD:
		if(system(path) != 0) {
			debug(D_WQ, "Could not thirdput %s, command (%s) failed. (%s)\n", filename, path, strerror(errno));
			return 0;
		}
		break;
	}
	link_putliteral(master, "thirdput complete\n", time(0) + active_timeout);
	return 1;

}

// Kill task if there's one running
static void kill_task() {
	if(task_status == TASK_RUNNING) {
		//make sure a few seconds have passed since child process was created to avoid sending a signal 
		//before it has been fully initialized. Else, the signal sent to that process gets lost.	
		timestamp_t elapsed_time_execution_start = timestamp_get() -
		execution_start;
		if (elapsed_time_execution_start/1000000 < 3)
			sleep(3 - (elapsed_time_execution_start/1000000));	
		
		debug(D_WQ, "terminating the current running task - process %d", pid);
		// Send signal to process group of child which is denoted by -ve value of child pid.
		// This is done to ensure delivery of signal to processes forked by the child. 
		kill((-1*pid), SIGKILL);
	}
}

static void kill_and_reap_task() {
	kill_task();
	// This reaps the killed child process created by function execute_task
	if(task_status == TASK_RUNNING) 
		wait_task_process(NULL);
}

static int do_kill() {
	kill_task();
	return 1;
}

static int do_release() {
	debug(D_WQ, "released by master at %s:%d.\n", actual_addr, actual_port);
	released_by_master = 1;
	return 0;
}

static int send_keepalive(struct link *master){
	link_putliteral(master, "alive\n", time(0) + active_timeout);
	debug(D_WQ, "sent response to keepalive check from master at %s:%d.\n", actual_addr, actual_port);
	return 1;
}


static void disconnect_master(struct link *master) {
	debug(D_WQ, "Disconnecting the current master ...\n");
	link_close(master);

	if(auto_worker) {
		record_bad_master(duplicate_work_queue_master(actual_master));
	}

	kill_and_reap_task();

	// Remove the contents of the workspace. 
	delete_dir_contents(workspace);

	if(released_by_master) {
		released_by_master = 0;
	} else {
		sleep(5);
	}
}

static void abort_worker() {
	// Free dynamically allocated memory
	if(stdout_buffer) {
		free(stdout_buffer);
		stdout_buffer = NULL;
		stdout_buffer_used = 0;
	}

	if(pool_name) {
		free(pool_name);
	}
	if(user_specified_workdir) {
		free(user_specified_workdir);
	} 
	free(os_name);
	free(arch_name);

	// Kill running task if any 
    kill_and_reap_task();	

	// Remove workspace. 
	fprintf(stdout, "work_queue_worker: cleaning up %s\n", workspace);
	delete_dir(workspace);
}

static int path_within_workspace(const char *path, const char *workspace) {
	if(!path) return 0;

	char absolute_workspace[PATH_MAX+1];
	if(!realpath(workspace, absolute_workspace)) {
		debug(D_WQ, "Failed to resolve the absolute path of workspace - %s: %s", path, strerror(errno));
		return 0;
	}	

	char *p;
	if(path[0] == '/') {
		p = strstr(path, absolute_workspace);
		if(p != path) {
			return 0;
		}
	}

	char absolute_path[PATH_MAX+1];
	char *tmp_path = xxstrdup(path);

	int rv = 1;
	while((p = strrchr(tmp_path, '/')) != NULL) {
		//debug(D_WQ, "Check if %s is within workspace - %s", tmp_path, absolute_workspace);
		*p = '\0';
		if(realpath(tmp_path, absolute_path)) {
			p = strstr(absolute_path, absolute_workspace);
			if(p != absolute_path) {
				rv = 0;
			}
			break;
		} else {
			if(errno != ENOENT) {
				debug(D_WQ, "Failed to resolve the absolute path of %s: %s", tmp_path, strerror(errno));
				rv = 0;
				break;
			}
		}
	}

	free(tmp_path);
	return rv;
}

static int handle_link(struct link *master) {
	char line[WORK_QUEUE_LINE_MAX];
	char filename[WORK_QUEUE_LINE_MAX];
	char path[WORK_QUEUE_LINE_MAX];
	INT64_T length;
	int mode, r;

	if(link_readline(master, line, sizeof(line), time(0)+short_timeout)) {
		debug(D_WQ, "received command: %s.\n", line);
		if(sscanf(line, "work %lld", &length)) {
			r = do_work(master, length);
		} else if(sscanf(line, "stat %s", filename) == 1) {
			r = do_stat(master, filename);
		} else if(sscanf(line, "symlink %s %s", path, filename) == 2) {
			r = do_symlink(path, filename);
		} else if(sscanf(line, "put %s %lld %o", filename, &length, &mode) == 3) {
			if(path_within_workspace(filename, workspace)) {
				r = do_put(master, filename, length, mode);
			} else {
				debug(D_WQ, "Path - %s is not within workspace %s.", filename, workspace);
				r= 0;
			}
		} else if(sscanf(line, "unlink %s", path) == 1) {
			if(path_within_workspace(filename, workspace)) {
				r = do_unlink(path);
			} else {
				debug(D_WQ, "Path - %s is not within workspace %s.", filename, workspace);
				r= 0;
			}
		} else if(sscanf(line, "mkdir %s %o", filename, &mode) == 2) {
			r = do_mkdir(filename, mode);
		} else if(sscanf(line, "rget %s", filename) == 1) {
			r = do_rget(master, filename);
		} else if(sscanf(line, "get %s", filename) == 1) {	// for backward compatibility
			r = do_get(master, filename);
		} else if(sscanf(line, "thirdget %d %s %[^\n]", &mode, filename, path) == 3) {
			r = do_thirdget(mode, filename, path);
		} else if(sscanf(line, "thirdput %d %s %[^\n]", &mode, filename, path) == 3) {
			r = do_thirdput(master, mode, filename, path);
		} else if(!strncmp(line, "kill", 5)){
			r = do_kill();
		} else if(!strncmp(line, "release", 8)) {
			r = do_release();
		} else if(!strncmp(line, "exit", 5)) {
			kill_and_reap_task();
			r = 0;
		} else if(!strncmp(line, "check", 6)) {
			r = send_keepalive(master);
		} else {
			debug(D_WQ, "Unrecognized master message: %s.\n", line);
			r = 0;
		}
	} else {
		debug(D_WQ, "Failed to read from master.\n");
		r = 0;
	}

	return r;
}

static void work_for_master(struct link *master) {
	if(!master) {
		return;
	}

	debug(D_WQ, "working for master at %s:%d.\n", actual_addr, actual_port);

	time_t idle_stoptime = time(0) + idle_timeout;
	// Start serving masters
	while(!abort_flag) {
		if(time(0) > idle_stoptime && task_status == TASK_NONE) {
			debug(D_NOTICE, "work_queue_worker: giving up because did not receive any task in %d seconds.\n", idle_timeout);
			abort_flag = 1;
			break;
		}

		int result = poll_master_and_task(master, pipefds[0], 5);

		if(result & POLL_FAIL) {
			abort_flag = 1;
			break;
		} 

		int ok = 1;
		if(result & PIPE_ACTIVE) {
			ok &= handle_task(master);
		} 

		if(result & LINK_ACTIVE) {
			ok &= handle_link(master);
		} 

		ok &= check_disk_space_for_filesize(0);

		if(!ok) {
			disconnect_master(master);
			break;
		}

		if(result != 0) {
			idle_stoptime = time(0) + idle_timeout;
		}
	}
}

static void handle_abort(int sig)
{
	abort_flag = 1;
}

static void show_help(const char *cmd)
{
	fprintf(stdout, "Use: %s [options] <masterhost> <port>\n", cmd);
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " -a             Enable auto mode. In this mode the worker would ask a catalog server for available masters.\n");
	fprintf(stdout, " -C <catalog>   Set catalog server to <catalog>. Format: HOSTNAME:PORT \n");
	fprintf(stdout, " -d <subsystem> Enable debugging for this subsystem.\n");
	fprintf(stdout, " -o <file>      Send debugging to this file.\n");
	fprintf(stdout, " -N <project>   Name of a preferred project. A worker can have multiple preferred projects.\n");
	fprintf(stdout, " -t <time>      Abort after this amount of idle time. (default=%ds)\n", idle_timeout);
	fprintf(stdout, " -w <size>      Set TCP window size.\n");
	fprintf(stdout, " -i <time>      Set initial value for backoff interval when worker fails to connect to a master. (default=%ds)\n", init_backoff_interval);
	fprintf(stdout, " -b <time>      Set maxmimum value for backoff interval when worker fails to connect to a master. (default=%ds)\n", max_backoff_interval);
	fprintf(stdout, " -z <size>      Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=%lluMB)\n", disk_avail_threshold);
	fprintf(stdout, " -A <arch>      Set architecture string for the worker to report to master instead of the value in uname (%s).\n", arch_name);
	fprintf(stdout, " -O <os>        Set operating system string for the worker to report to master instead of the value in uname (%s).\n", os_name);
	fprintf(stdout, " -s <path>      Set the location for creating the working directory of the worker.\n");
	fprintf(stdout, " -v             Show version string\n");
	fprintf(stdout, " -h             Show this help screen\n");
}

static void check_arguments(int argc, char **argv) {
	const char *host = NULL;

	if(!auto_worker) {
		if((argc - optind) != 2) {
			show_help(argv[0]);
			exit(1);
		}
		host = argv[optind];
		actual_port = atoi(argv[optind + 1]);

		if(!domain_name_cache_lookup(host, actual_addr)) {
			fprintf(stderr, "couldn't lookup address of host %s\n", host);
			exit(1);
		}
	}

	if(auto_worker && !list_size(preferred_masters) && !pool_name) {
		fprintf(stderr, "Worker is running under auto mode. But no preferred master name is specified.\n");
		fprintf(stderr, "Please specify the preferred master names with the -N option.\n");
		exit(1);
	}

	if(!catalog_server_host) {
		catalog_server_host = CATALOG_HOST;
		catalog_server_port = CATALOG_PORT;
	}
}

static int setup_workspace() {
	// Setup working space(dir)
	const char *workdir;
	if (user_specified_workdir){
		workdir = user_specified_workdir;	
	} else if(getenv("_CONDOR_SCRATCH_DIR")) {
		workdir = getenv("_CONDOR_SCRATCH_DIR");
	} else if(getenv("TEMP")) {
		workdir = getenv("TEMP");
	} else {
		workdir = "/tmp";
	}

	sprintf(workspace, "%s/worker-%d-%d", workdir, (int) getuid(), (int) getpid());
	if(mkdir(workspace, 0700) == -1) {
		return 0;
	} 

	fprintf(stdout, "work_queue_worker: working in %s\n", workspace);
	return 1;
}



int main(int argc, char *argv[])
{
	char c;
	int w;
	struct utsname uname_data;
	struct link *master = NULL;

	preferred_masters = list_create();
	if(!preferred_masters) {
		fprintf(stderr, "Cannot allocate memory to store preferred work queue masters names.\n");
		exit(1);
	}
	
	//obtain the architecture and os on which worker is running.
	uname(&uname_data);
	os_name = xxstrdup(uname_data.sysname);
	arch_name = xxstrdup(uname_data.machine);

	debug_config(argv[0]);

	while((c = getopt(argc, argv, "aC:d:t:o:p:N:w:i:b:z:A:O:s:vh")) != (char) -1) {
		switch (c) {
		case 'a':
			auto_worker = 1;
			break;
		case 'C':
			if(!parse_catalog_server_description(optarg, &catalog_server_host, &catalog_server_port)) {
				fprintf(stderr, "The provided catalog server is invalid. The format of the '-C' option is '-C HOSTNAME:PORT'.\n");
				exit(1);
			}
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 't':
			idle_timeout = string_time_parse(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'N':
			list_push_tail(preferred_masters, strdup(optarg));
			break;
		case 'p':
			pool_name = xxstrdup(optarg);
			break;
		case 'w':
			w = string_metric_parse(optarg);
			link_window_set(w, w);
			break;
		case 'i':
			init_backoff_interval = string_metric_parse(optarg);
			break;
		case 'b':
			max_backoff_interval = string_metric_parse(optarg);
			if (max_backoff_interval < init_backoff_interval) {
				fprintf(stderr, "Maximum backoff interval provided must be greater than the initial backoff interval of %ds.\n", init_backoff_interval);
				exit(1);
			}
			break;
		case 'z':
			disk_avail_threshold = atoll(optarg) * MEGA;
			break;
		case 'A':
			free(arch_name); //free the arch string obtained from uname
			arch_name = xxstrdup(optarg);
			break;
		case 'O':
			free(os_name); //free the os string obtained from uname
			os_name = xxstrdup(optarg);
			break;
		case 's':
			user_specified_workdir = xxstrdup(optarg);
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			exit(EXIT_SUCCESS);
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	check_arguments(argc, argv);

	signal(SIGTERM, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGINT, handle_abort);

	srand((unsigned int) (getpid() ^ time(NULL)));
	bad_masters = hash_cache_create(127, hash_string, (hash_cache_cleanup_t)free_work_queue_master);
	stdout_buffer = xxmalloc(STDOUT_BUFFER_SIZE);
	
	if(!setup_workspace()) {
		fprintf(stderr, "work_queue_worker: failed to setup workspace at %s.\n", workspace);
		exit(1);
	}

	// set $WORK_QUEUE_SANDBOX to workspace.
	debug(D_WQ, "WORK_QUEUE_SANDBOX set to %s.\n", workspace);
	setenv("WORK_QUEUE_SANDBOX", workspace, 0);

	// change to workspace
	chdir(workspace);
	
	if(!check_disk_space_for_filesize(0)) {
		goto abort;
	}

	while(!abort_flag) {
		if((master = connect_master(time(0) + idle_timeout)) == NULL) {
			break;
		}
		work_for_master(master);
	}

abort:
	abort_worker();
	return 0;
}
