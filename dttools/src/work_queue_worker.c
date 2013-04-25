/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"
#include "work_queue_protocol.h"
#include "work_queue_internal.h"

#include "cctools.h"
#include "macros.h"
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
#include "link_auth.h"
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
#include "itable.h"
#include "random_init.h"
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

#define MIN_TERMINATE_BOUNDARY 0 
#define TERMINATE_BOUNDARY_LEEWAY 30

#define PIPE_ACTIVE 1
#define LINK_ACTIVE 2
#define POLL_FAIL 4

#define TASK_NONE 0
#define TASK_RUNNING 1

#define WORKER_MODE_AUTO    0
#define WORKER_MODE_CLASSIC 1
#define WORKER_MODE_WORKER  2
#define WORKER_MODE_FOREMAN 3


struct task_info {
	int taskid;
	pid_t pid;
	int status;
	
	struct rusage *rusage;
	timestamp_t execution_start;
	timestamp_t execution_end;
	
	char *output_file_name;
	int output_fd;
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

// Chance that a worker will decide to shut down each minute without warning, to simulate failure.
static double worker_volatility = 0.0;

// Flag gets set on receipt of a terminal signal.
static int abort_flag = 0;

// Threshold for available disk space (MB) beyond which clean up and restart.
static UINT64_T disk_avail_threshold = 100;

// Terminate only when:
// terminate_boundary - (current_time - worker_start_time)%terminate_boundary ~~ 0
// Use case: hourly charge resource such as Amazon EC2
static int terminate_boundary = 0;

// Password shared between master and worker.
char *password = 0;

// Basic worker global variables
static int worker_mode = WORKER_MODE_CLASSIC;
static int worker_mode_default = WORKER_MODE_CLASSIC;
static char actual_addr[LINK_ADDRESS_MAX];
static int actual_port;
static char workspace[WORKER_WORKSPACE_NAME_MAX];
static char *os_name = NULL; 
static char *arch_name = NULL;
static char *user_specified_workdir = NULL;
static time_t worker_start_time = 0;
static UINT64_T current_taskid = 0;
static char *base_debug_filename = NULL;

// Foreman mode global variables
static struct work_queue *foreman_q = NULL;
static struct itable *unfinished_tasks = NULL;

// Forked task related
static int task_status= TASK_NONE;
static int max_worker_tasks = 1;
static int max_worker_tasks_default = 1;
static int current_worker_tasks = 0;
static struct itable *active_tasks = NULL;
static struct itable *stored_tasks = NULL;


// Catalog mode control variables
static char *catalog_server_host = NULL;
static int catalog_server_port = 0;
static int auto_worker = 0;
static char *pool_name = NULL;
static struct work_queue_master *actual_master = NULL;
static struct list *preferred_masters = NULL;
static struct hash_cache *bad_masters = NULL;
static int released_by_master = 0;
static char *current_project = NULL;

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

	link_putfstring(master, "ready %s %d %llu %llu %llu %llu %s %s %s %s %s %s \n", time(0) + active_timeout, hostname, ncpus, memory_avail, memory_total, disk_avail, disk_total, name_of_master, name_of_pool, os_name, arch_name, workspace, CCTOOLS_VERSION);
	
	if(worker_mode == WORKER_MODE_WORKER || worker_mode == WORKER_MODE_FOREMAN) {	
		current_worker_tasks = max_worker_tasks;
		link_putfstring(master, "update slots %d\n", time(0)+active_timeout, max_worker_tasks);
	}
}

static void clear_task_info(struct task_info *ti)
{
	if(ti->output_fd) {
		close(ti->output_fd);
		unlink(ti->output_file_name);
	}
	if(ti->rusage) {
		free(ti->rusage);
	}
	
	memset(ti, 0, sizeof(*ti));
}

static const char task_output_template[] = "./worker.stdout.XXXXXX";

static pid_t execute_task(const char *cmd, struct task_info *ti)
{
	fflush(NULL); /* why is this necessary? */

	ti->output_file_name = strdup(task_output_template);
	ti->output_fd = mkstemp(ti->output_file_name);
	if (ti->output_fd == -1) {
		debug(D_WQ, "Could not open worker stdout: %s", strerror(errno));
		return 0;
	}

	ti->execution_start = timestamp_get();

	ti->pid = fork();
	
	if(ti->pid > 0) {
		// Make child process the leader of its own process group. This allows
		// signals to also be delivered to processes forked by the child process.
		// This is currently used by kill_task(). 
		setpgid(ti->pid, 0); 
		
		debug(D_WQ, "started process %d: %s", ti->pid, cmd);
		return ti->pid;
	} else if(ti->pid < 0) {
		debug(D_WQ, "couldn't create new process: %s\n", strerror(errno));
		unlink(ti->output_file_name);
		close(ti->output_fd);
		return ti->pid;
	} else {
		int fd = open("/dev/null", O_RDONLY);
		if (fd == -1) fatal("could not open /dev/null: %s", strerror(errno));
		int result = dup2(fd, STDIN_FILENO);
		if (result == -1) fatal("could not dup /dev/null to stdin: %s", strerror(errno));

		result = dup2(ti->output_fd, STDOUT_FILENO);
		if (result == -1) fatal("could not dup pipe to stdout: %s", strerror(errno));

		result = dup2(ti->output_fd, STDERR_FILENO);
		if (result == -1) fatal("could not dup pipe to stderr: %s", strerror(errno));

		close(ti->output_fd);

		execlp("sh", "sh", "-c", cmd, (char *) 0);
		_exit(127);	// Failed to execute the cmd.
	}
	return 0;
}

static void report_task_complete(struct link *master, struct task_info *ti, struct work_queue_task *t)
{
	INT64_T output_length;
	struct stat st;

	if(ti) {
		fstat(ti->output_fd, &st);
		output_length = st.st_size;
		lseek(ti->output_fd, 0, SEEK_SET);
		debug(D_WQ, "Task complete: result %d %lld %llu %d", ti->status, output_length, ti->execution_end - ti->execution_start, ti->taskid);
		link_putfstring(master, "result %d %lld %llu %d\n", time(0) + active_timeout, ti->status, output_length, ti->execution_end-ti->execution_start, ti->taskid);
		link_stream_from_fd(master, ti->output_fd, output_length, time(0)+active_timeout);
	} else if(t) {
		if(t->output) {
			output_length = strlen(t->output);
		} else {
			output_length = 0;
		}
		debug(D_WQ, "Task complete: result %d %lld %llu %d", t->return_status, output_length, t->cmd_execution_time, t->taskid);
		link_putfstring(master, "result %d %lld %llu %d\n", time(0) + active_timeout, t->return_status, output_length, t->cmd_execution_time, t->taskid);
		if(output_length) {
			link_putlstring(master, t->output, output_length, time(0)+active_timeout);
		}
	}
}

static int handle_tasks(struct link *master) {
	struct task_info *ti;
	pid_t pid;
	int result = 0;
	
	itable_firstkey(active_tasks);
	while(itable_nextkey(active_tasks, (UINT64_T*)&pid, (void**)&ti)) {
		struct rusage rusage;
		result = wait4(pid, &ti->status, WNOHANG, &rusage);
		if(result) {
			if(result < 0) {
				debug(D_WQ, "Error checking on child process (%d).", ti->pid);
				abort_flag = 1;
				return 0;
			}
			if (!WIFEXITED(ti->status)){
				debug(D_WQ, "Task (process %d) did not exit normally.\n", ti->pid);
			}
			
			ti->rusage = malloc(sizeof(rusage));
			memcpy(ti->rusage, &rusage, sizeof(rusage));
			ti->execution_end = timestamp_get();
			
			itable_remove(active_tasks, ti->pid);
			itable_remove(stored_tasks, ti->taskid);
			itable_firstkey(active_tasks);
			
			report_task_complete(master, ti, NULL);
			clear_task_info(ti);
			free(ti);
		}
		
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

static int foreman_finish_task(struct link *master, int taskid, int length) {
	struct work_queue_task *t;
	char * cmd;

	cmd = malloc((length+1) * sizeof(*cmd));
	memset(cmd, 0, length+1);

	link_read(master, cmd, length, time(0) + active_timeout);
	

	t = (struct work_queue_task *)itable_remove(unfinished_tasks, taskid);
	if(!t) {
		t = work_queue_task_create(cmd);
	} else {
		free(t->command_line);
		t->command_line = cmd;
	}

	work_queue_submit(foreman_q, t);
	t->taskid = taskid;
	return 1;
}

static int foreman_add_file_to_task(const char *filename, int taskid, int type, int flags) {
	struct work_queue_task *t;

	t = (struct work_queue_task *)itable_lookup(unfinished_tasks, taskid);
	if(!t) {
		t = work_queue_task_create("");
		itable_insert(unfinished_tasks, taskid, t);
	}

	work_queue_task_specify_file(t, filename, filename, type, flags);
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

			if(current_project) free(current_project);
			current_project = strdup(m->proj);
			
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

		if(master) {
			link_tune(master,LINK_TUNE_INTERACTIVE);
			if(password) {
				debug(D_WQ,"authenticating to master");
				if(!link_auth_password(master,password,time(0) + master_timeout)) {
					fprintf(stderr,"work_queue_worker: wrong password for master %s:%d\n",actual_addr,actual_port);
					link_close(master);
					master = 0;
				}
			}
		}

		if(!master) {
			if (backoff_interval > max_backoff_interval) {
				backoff_interval = max_backoff_interval;
			}
		
			sleep(backoff_interval);
			backoff_interval *= backoff_multiplier;
			continue;
		}

		report_worker_ready(master);

		//reset backoff interval after connection to master.
		backoff_interval = init_backoff_interval; 

		debug(D_WQ, "connected to master %s:%d", actual_addr, actual_port);
		return master;
	}

	return NULL;
}


static int do_work(struct link *master, int taskid, INT64_T length) {
	char *cmd = malloc(length + 10);
	struct task_info *ti;

	link_read(master, cmd, length, time(0) + active_timeout);
	cmd[length] = 0;

	debug(D_WQ, "%s", cmd);
	
	ti = malloc(sizeof(*ti));
	memset(ti, 0, sizeof(*ti));
	
	ti->taskid = taskid;
	execute_task(cmd, ti);
	free(cmd);
	
	if(ti->pid < 0) {
		fprintf(stderr, "work_queue_worker: failed to fork task. Shutting down worker...\n");
		free(ti);
		abort_flag = 1;
		return 0;
	}

	//snprintf(ti->output_file_name, 50, "%d.task.stdout.tmp", ti->pid);

	task_status = ti->status = TASK_RUNNING;
	itable_insert(stored_tasks, taskid, ti);
	itable_insert(active_tasks, ti->pid, ti);
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
		debug(D_WQ, "Could not put file %s, not enough disk space (%lld bytes needed)\n", filename, length);
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
		debug(D_WQ, "Failed to put file - %s (%s)\n", filename, strerror(errno));
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

static void kill_task(struct task_info *ti) {
	
	//make sure a few seconds have passed since child process was created to avoid sending a signal 
	//before it has been fully initialized. Else, the signal sent to that process gets lost.	
	timestamp_t elapsed_time_execution_start = timestamp_get() - ti->execution_start;
	
	if (elapsed_time_execution_start/1000000 < 3)
		sleep(3 - (elapsed_time_execution_start/1000000));	
	
	debug(D_WQ, "terminating the current running task - process %d", ti->pid);
	// Send signal to process group of child which is denoted by -ve value of child pid.
	// This is done to ensure delivery of signal to processes forked by the child. 
	kill((-1*ti->pid), SIGKILL);
	
	// Reap the child process to avoid zombies.
	waitpid(ti->pid, NULL, 0);
	
	// Clean up the task info structure.
	itable_remove(stored_tasks, ti->taskid);
	itable_remove(active_tasks, ti->pid);
	clear_task_info(ti);
	free(ti);
}

static void kill_all_tasks() {
	struct task_info *ti;
	pid_t pid;
	UINT64_T taskid;
	
	// If there are no stored tasks, then there are no active tasks, return. 
	if(!stored_tasks) return;
	// If there are no active local tasks, return.
	if(!active_tasks) return;
	
	// Send kill signal to all child processes
	itable_firstkey(active_tasks);
	while(itable_nextkey(active_tasks, (UINT64_T*)&pid, (void**)&ti)) {
		kill(-1 * ti->pid, SIGKILL);
	}
	
	// Wait for all children to return, and remove them from the active tasks list.
	while(itable_size(active_tasks)) {
		pid = wait(0);
		ti = itable_remove(active_tasks, pid);
		if(ti) {
			itable_remove(stored_tasks, ti->taskid);
			clear_task_info(ti);
			free(ti);
		}
	}
	
	// Clear out the stored tasks list if any are left.
	itable_firstkey(stored_tasks);
	while(itable_nextkey(stored_tasks, &taskid, (void**)&ti)) {
		clear_task_info(ti);
		free(ti);
	}
	itable_clear(stored_tasks);
}

static int do_kill(int taskid) {
	struct task_info *ti = itable_lookup(stored_tasks, taskid);
	kill_task(ti);
	return 1;
}

static int do_release() {
	debug(D_WQ, "released by master at %s:%d.\n", actual_addr, actual_port);

	if(base_debug_filename && getenv("WORK_QUEUE_RESET_DEBUG_FILE")) {
		char debug_filename[WORK_QUEUE_LINE_MAX];
		
		sprintf(debug_filename, "%s.%s", base_debug_filename, current_project);
		debug_config_file(NULL);
		rename(base_debug_filename, debug_filename);
		debug_config_file(base_debug_filename);
	}
	
	released_by_master = 1;
	return 0;
}

static int do_reset() {
	
	if(worker_mode == WORKER_MODE_FOREMAN) {
		work_queue_reset(foreman_q, 0);
	} else {
		kill_all_tasks();
	}
	
	if(delete_dir_contents(workspace) < 0) {
		return 0;
	}
		
	return 1;
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

	kill_all_tasks();

	if(foreman_q) {
		work_queue_reset(foreman_q, 0);
	}

	// Remove the contents of the workspace.
	delete_dir_contents(workspace);

	// Clean up any remaining tasks.
	if(unfinished_tasks) {
		UINT64_T taskid;
		struct work_queue_task *t;
		itable_firstkey(unfinished_tasks);
		while(itable_nextkey(unfinished_tasks, &taskid, (void**)&t)) {
			work_queue_task_delete(t);
		}
		itable_clear(unfinished_tasks);
	}
	
	worker_mode = worker_mode_default;
	current_worker_tasks = 0;
	max_worker_tasks = max_worker_tasks_default;
	
	if(released_by_master) {
		released_by_master = 0;
	} else {
		sleep(5);
	}
}

static void abort_worker() {
	// Free dynamically allocated memory
	if(pool_name) {
		free(pool_name);
	}
	if(user_specified_workdir) {
		free(user_specified_workdir);
	} 
	free(os_name);
	free(arch_name);

	// Kill all running tasks
	kill_all_tasks();

	if(foreman_q) {
		work_queue_delete(foreman_q);
	}
	
	if(unfinished_tasks) {
		UINT64_T taskid;
		struct work_queue_task *t;
		itable_firstkey(unfinished_tasks);
		while(itable_nextkey(unfinished_tasks, &taskid, (void**)&t)) {
			work_queue_task_delete(t);
		}
		itable_delete(unfinished_tasks);
	}
	
	if(active_tasks) {
		itable_delete(active_tasks);
	}
	if(stored_tasks) {
		itable_delete(stored_tasks);
	}

	// Remove workspace. 
	fprintf(stdout, "work_queue_worker: cleaning up %s\n", workspace);
	delete_dir(workspace);
}

static void update_worker_status(struct link *master) {
	if(current_worker_tasks != max_worker_tasks) {
		current_worker_tasks = max_worker_tasks;
		link_putfstring(master, "update slots %d\n", time(0)+active_timeout, max_worker_tasks);
	}
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

static int worker_handle_master(struct link *master) {
	char line[WORK_QUEUE_LINE_MAX];
	char filename[WORK_QUEUE_LINE_MAX];
	char path[WORK_QUEUE_LINE_MAX];
	INT64_T length;
	INT64_T taskid = 0;
	int flags = WORK_QUEUE_NOCACHE;
	int mode, r, n;

	if(link_readline(master, line, sizeof(line), time(0)+short_timeout)) {
		debug(D_WQ, "received command: %s.\n", line);
		if((n = sscanf(line, "work %" SCNd64 "%" SCNd64, &length, &taskid))) {
			if(n < 2) {
				current_taskid++;
				r = do_work(master, current_taskid, length);
			} else {
				r = do_work(master, taskid, length);
			}
		} else if(sscanf(line, "stat %s", filename) == 1) {
			r = do_stat(master, filename);
		} else if(sscanf(line, "symlink %s %s", path, filename) == 2) {
			r = do_symlink(path, filename);
		} else if(sscanf(line, "need %" SCNd64 " %s %d", &taskid, filename, &flags) == 3) {
			r = 1;
		} else if((n = sscanf(line, "put %s %" SCNd64 " %o %" SCNd64 " %d", filename, &length, &mode, &taskid, &flags)) >= 3) {
			if(path_within_workspace(filename, workspace)) {
				if(length >= 0) {
					r = do_put(master, filename, length, mode);
				} else {
					r = 1;
				}
			} else {
				debug(D_WQ, "Path - %s is not within workspace %s.", filename, workspace);
				r= 0;
			}
		} else if(sscanf(line, "unlink %s", filename) == 1) {
			if(path_within_workspace(filename, workspace)) {
				r = do_unlink(filename);
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
		} else if(sscanf(line, "thirdget %o %s %[^\n]", &mode, filename, path) == 3) {
			r = do_thirdget(mode, filename, path);
		} else if(sscanf(line, "thirdput %o %s %[^\n]", &mode, filename, path) == 3) {
			r = do_thirdput(master, mode, filename, path);
		} else if(!strcmp("kill", line)){
			if(sscanf(line, "kill %" SCNd64, &taskid) == 0) {
				taskid = current_taskid;
			}
			if(taskid >= 0) {
				r = do_kill(taskid);
			} else {
				kill_all_tasks();
				r = 1;
			}
		} else if(!strncmp(line, "release", 8)) {
			r = do_release();
		} else if(!strncmp(line, "exit", 5)) {
			r = 0;
		} else if(!strncmp(line, "check", 6)) {
			r = send_keepalive(master);
		} else if(!strncmp(line, "reset", 5)) {
			r = do_reset();
		} else if(!strncmp(line, "auth", 4)) {
			fprintf(stderr,"work_queue_worker: this master requires a password. (use the -P option)\n");
			r = 0;
		} else if(!strncmp(line, "update", 6)) {
			worker_mode = WORKER_MODE_WORKER;
			update_worker_status(master);
			r = 1;
		} else {
			debug(D_WQ, "Unrecognized master message: %s.\n", line);
			r = 0;
		}
		
		if(!worker_mode && taskid) {
			worker_mode = WORKER_MODE_WORKER;
		}
	} else {
		debug(D_WQ, "Failed to read from master.\n");
		r = 0;
	}

	return r;
}

static void work_for_master(struct link *master) {
	int result;
	sigset_t mask;

	if(!master) {
		return;
	}

	debug(D_WQ, "working for master at %s:%d.\n", actual_addr, actual_port);

	sigemptyset(&mask);
	sigaddset(&mask, SIGCHLD);
	
	time_t idle_stoptime = time(0) + idle_timeout;
	time_t volatile_stoptime = time(0) + 60;
	// Start serving masters
	while(!abort_flag) {
		if(time(0) > idle_stoptime && itable_size(stored_tasks) == 0) {
			debug(D_NOTICE, "work_queue_worker: giving up because did not receive any task in %d seconds.\n", idle_timeout);
			abort_flag = 1;
			break;
		}
		
		if(worker_volatility && time(0) > volatile_stoptime) {
			if( (double)rand()/(double)RAND_MAX < worker_volatility) {
				debug(D_NOTICE, "work_queue_worker: disconnect from master due to volatility check.\n");
				disconnect_master(master);
				break;
			} else {
				volatile_stoptime = time(0) + 60;
			}
		}

		result = link_usleep_mask(master, 5000, &mask, 1, 0);

		if(result < 0) {
			abort_flag = 1;
			break;
		}
		
		if(worker_mode == WORKER_MODE_WORKER) {
			update_worker_status(master);
		}
		
		int ok = 1;
		if(result) {
			ok &= worker_handle_master(master);
		}

		ok &= handle_tasks(master);

		ok &= check_disk_space_for_filesize(0);

		if(!ok) {
			disconnect_master(master);
			break;
		}

		if(result != 0 || itable_size(active_tasks)) {
			idle_stoptime = time(0) + idle_timeout;
		}
	}
}

static int foreman_handle_master(struct link *master) {
	char line[WORK_QUEUE_LINE_MAX];
	char filename[WORK_QUEUE_LINE_MAX];
	INT64_T length;
	INT64_T taskid;
	int mode, flags, r;

	if(link_readline(master, line, sizeof(line), time(0)+short_timeout)) {
		debug(D_WQ, "received command: %s.\n", line);
		if(sscanf(line, "work %" SCNd64 "%" SCNd64, &length, &taskid) == 2) {
			r = foreman_finish_task(master, taskid, length);
		} else if(sscanf(line, "put %s %" SCNd64 " %o %" SCNd64 " %d", filename, &length, &mode, &taskid, &flags) == 5) {
			if(path_within_workspace(filename, workspace)) {
				if(length >= 0) {
					r = do_put(master, filename, length, mode);
				} else {
					r = 1;
				}
				foreman_add_file_to_task(filename, taskid, WORK_QUEUE_INPUT, flags);
			} else {
				debug(D_WQ, "Path - %s is not within workspace %s.", filename, workspace);
				r= 0;
			}
		} else if(sscanf(line, "need %" SCNd64 " %s %d", &taskid, filename, &flags) == 3) {
			if(path_within_workspace(filename, workspace)) {
				foreman_add_file_to_task(filename, taskid, WORK_QUEUE_OUTPUT, flags);
				r=1;
			} else {
				debug(D_WQ, "Path - %s is not within workspace %s.", filename, workspace);
				r=0;
			}
		} else if(sscanf(line, "unlink %s", filename) == 1) {
			if(path_within_workspace(filename, workspace)) {
				r = do_unlink(filename);
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
		} else if(sscanf(line, "kill %" SCNd64 , &taskid) == 1){
			struct work_queue_task *t;
			t = work_queue_cancel_by_taskid(foreman_q, taskid);
			if(t) {
				work_queue_task_delete(t);
			}
			r = 1;
		} else if(!strncmp(line, "release", 8)) {
			r = do_release();
		} else if(!strncmp(line, "exit", 5)) {
			r = 0;
		} else if(!strncmp(line, "check", 6)) {
			r = send_keepalive(master);
		} else if(!strncmp(line, "reset", 5)) {
			r = do_reset();
		} else if(!strncmp(line, "auth", 4)) {
			fprintf(stderr,"work_queue_worker: this master requires a password. (use the -P option)\n");
			r = 0;
		} else if(!strncmp(line, "update", 6)) {
			update_worker_status(master);
			r = 1;
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

static void foreman_for_master(struct link *master) {
	static struct list *master_link = NULL;
	static struct link *current_master = NULL;
	static struct list *master_link_active = NULL;
	struct work_queue_stats s;

	if(!master) {
		return;
	}

	if(!master_link) {
		master_link = list_create();
		master_link_active = list_create();
	}
	
	if(master != current_master) {
		while(list_pop_head(master_link));
		list_push_tail(master_link, master);
		current_master = master;
	}

	debug(D_WQ, "working for master at %s:%d as foreman.\n", actual_addr, actual_port);

	time_t idle_stoptime = time(0) + idle_timeout;

	while(!abort_flag) {
		int result = 1;
		struct work_queue_task *task = NULL;

		if(time(0) > idle_stoptime && task_status == TASK_NONE) {
			debug(D_NOTICE, "work_queue_worker: giving up because did not receive any task in %d seconds.\n", idle_timeout);
			abort_flag = 1;
			break;
		}

		task = work_queue_wait_internal(foreman_q, short_timeout, master_link, master_link_active);
		
		if(task) {
			report_task_complete(master, NULL, task);
			work_queue_task_delete(task);
			result = 1;
		}

		work_queue_get_stats(foreman_q, &s);
		max_worker_tasks = s.workers_ready + s.workers_busy + s.workers_full;

		update_worker_status(master);
		
		if(list_size(master_link_active)) {
			result &= foreman_handle_master(list_pop_head(master_link_active));
		}

		if(!result) {
			disconnect_master(master);
			current_master = NULL;
			break;
		}
		
		if(result) {
			idle_stoptime = time(0) + idle_timeout;
		}
	}
}

static void handle_abort(int sig)
{
	abort_flag = 1;
}

static void handle_sigchld(int sig)
{
	return;
}

static void show_help(const char *cmd)
{
	fprintf(stdout, "Use: %s [options] <masterhost> <port>\n", cmd);
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " -a                      Enable auto mode. In this mode the worker\n");
	fprintf(stdout, "                         would ask a catalog server for available masters.\n");
	fprintf(stdout, " -C <catalog>            Set catalog server to <catalog>. Format: HOSTNAME:PORT \n");
	fprintf(stdout, " -d <subsystem>          Enable debugging for this subsystem.\n");
	fprintf(stdout, " -o <file>               Send debugging to this file.\n");
	fprintf(stdout, " --debug-file-size       Set the maximum size of the debug log (default 10M, 0 disables).\n");
	fprintf(stdout, " --debug-release-reset   Debug file will be closed, renamed, and a new one opened after being released from a master.\n");
	fprintf(stdout, " -m <mode>               Choose worker mode.\n");
	fprintf(stdout, "                         Can be [w]orker, [f]oreman, [c]lassic, or [a]uto (default=auto).\n");
	fprintf(stdout, " -f <port>[:<high_port>] Set the port for the foreman to listen on.  If <highport> is specified\n");
	fprintf(stdout, "                         the port is chosen from the range port:highport\n");
	fprintf(stdout, " -c, --measure-capacity	  Enable the measurement of foreman capacity to handle new workers (default=disabled).\n");
	fprintf(stdout, " -F, --fast-abort <mult>	  Set the fast abort multiplier for foreman (default=disabled).\n");
	fprintf(stdout, " --specify-log <logfile>  Send statistics about foreman to this file.\n");
	fprintf(stdout, " -M <project>            Name of a preferred project. A worker can have multiple preferred projects.\n");
	fprintf(stdout, " -N <project>            When in Foreman mode, the name of the project to advertise as.  In worker/classic/auto mode acts as '-M'.\n");
	fprintf(stdout, " -P,--password <pwfile>  Password file for authenticating to the master.\n");
	fprintf(stdout, " -t <time>               Abort after this amount of idle time. (default=%ds)\n", idle_timeout);
	fprintf(stdout, " -w <size>               Set TCP window size.\n");
	fprintf(stdout, " -i <time>               Set initial value for backoff interval when worker fails to connect to a master. (default=%ds)\n", init_backoff_interval);
	fprintf(stdout, " -b <time>               Set maxmimum value for backoff interval when worker fails to connect to a master. (default=%ds)\n", max_backoff_interval);
	fprintf(stdout, " -z <size>               Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=%" PRIu64 "MB)\n", disk_avail_threshold);
	fprintf(stdout, " -A <arch>               Set architecture string for the worker to report to master instead of the value in uname (%s).\n", arch_name);
	fprintf(stdout, " -O <os>                 Set operating system string for the worker to report to master instead of the value in uname (%s).\n", os_name);
	fprintf(stdout, " -s <path>               Set the location for creating the working directory of the worker.\n");
	fprintf(stdout, " -v                      Show version string\n");
	fprintf(stdout, " --volatility <chance>   Set the percent chance a worker will decide to shut down every minute.\n");
	fprintf(stdout, " --bandwidth <mult>      Set the multiplier for how long outgoing and incoming data transfers will take.\n");
	fprintf(stdout, " -h                      Show this help screen\n");
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

#define LONG_OPT_DEBUG_FILESIZE 'z'+1
#define LONG_OPT_VOLATILITY     'z'+2
#define LONG_OPT_BANDWIDTH      'z'+3
#define LONG_OPT_DEBUG_RELEASE  'z'+4
#define LONG_OPT_SPECIFY_LOG    'z'+5

struct option long_options[] = {
	{"password",            required_argument,  0,  'P'},
	{"debug-file-size",     required_argument,  0,   LONG_OPT_DEBUG_FILESIZE},
	{"volatility",          required_argument,  0,   LONG_OPT_VOLATILITY},
	{"bandwidth",           required_argument,  0,   LONG_OPT_BANDWIDTH},
	{"debug-release-reset", no_argument,        0,   LONG_OPT_DEBUG_RELEASE},
	{"measure-capacity",    no_argument,        0,   'c'},
	{"fast-abort",          required_argument,  0,   'F'},
	{"debug-file-size",     required_argument,  0,   LONG_OPT_SPECIFY_LOG},
	{0,0,0,0}
};

int main(int argc, char *argv[])
{
	char c;
	int w;
	int foreman_port = -1;
	char * foreman_name = NULL;
	struct utsname uname_data;
	struct link *master = NULL;
	int enable_capacity = 0;
	double fast_abort_multiplier = 0;
	char *foreman_stats_filename = NULL;

	worker_start_time = time(0);

	preferred_masters = list_create();
	if(!preferred_masters) {
		fprintf(stderr, "Cannot allocate memory to store preferred work queue masters names.\n");
		exit(1);
	}
	
	//obtain the architecture and os on which worker is running.
	uname(&uname_data);
	os_name = xxstrdup(uname_data.sysname);
	arch_name = xxstrdup(uname_data.machine);
	worker_mode = WORKER_MODE_AUTO;

	debug_config(argv[0]);

	while((c = getopt_long(argc, argv, "aB:cC:d:f:F:t:j:o:p:m:M:N:P:w:i:b:z:A:O:s:vh", long_options, 0)) != (char) -1) {
		switch (c) {
		case 'a':
			auto_worker = 1;
			break;
		case 'B':
			terminate_boundary = MAX(MIN_TERMINATE_BOUNDARY, string_time_parse(optarg));
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
		case LONG_OPT_DEBUG_FILESIZE:
			debug_config_file_size(MAX(0, string_metric_parse(optarg)));
			break;
		case 'f':
		{	char *low_port = optarg;
			char *high_port= strchr(optarg, ':');
			
			worker_mode = WORKER_MODE_FOREMAN;
			
			if(high_port) {
				*high_port = '\0';
				high_port++;
			} else {
				foreman_port = atoi(low_port);
				break;
			} 
			setenv("WORK_QUEUE_LOW_PORT", low_port, 0);
			setenv("WORK_QUEUE_HIGH_PORT", high_port, 0);
			foreman_port = -1;
			break;
		}
		case 'c':
			enable_capacity = 1; 
			break;
		case 'F':
			fast_abort_multiplier = atof(optarg); 
			break;
		case LONG_OPT_SPECIFY_LOG:
			foreman_stats_filename = xxstrdup(optarg); 
			break;
		case 't':
			idle_timeout = string_time_parse(optarg);
			break;
		case 'j':
			max_worker_tasks = max_worker_tasks_default = atoi(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			base_debug_filename = strdup(optarg);
			break;
		case 'm':
			if(!strncmp("foreman", optarg, 7) || optarg[0] == 'f') {
				worker_mode = worker_mode_default = WORKER_MODE_FOREMAN;
			} else if(!strncmp("worker", optarg, 6) || optarg[0] == 'w') {
				worker_mode = worker_mode_default = WORKER_MODE_WORKER;
			} else if(!strncmp("classic", optarg, 7) || optarg[0] == 'c') {
				worker_mode = worker_mode_default = WORKER_MODE_CLASSIC;
			}
			break;
		case 'M':
			auto_worker = 1;
			list_push_tail(preferred_masters, strdup(optarg));
			break;
		case 'N':
			auto_worker = 1;
			if(foreman_name) { // for backward compatibility with old syntax for specifying a worker's project name
				list_push_tail(preferred_masters, foreman_name);
			}
			foreman_name = strdup(optarg);
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
			break;
		case 'P':
		  	if(copy_file_to_buffer(optarg, &password) < 0) {
              fprintf(stderr,"work_queue_worker: couldn't load password from %s: %s\n",optarg,strerror(errno));
				return 1;
			}
			break;
		case LONG_OPT_VOLATILITY:
			worker_volatility = atof(optarg);
			break;
		case LONG_OPT_BANDWIDTH:
			setenv("WORK_QUEUE_BANDWIDTH", optarg, 1);
			break;
		case LONG_OPT_DEBUG_RELEASE:
			setenv("WORK_QUEUE_RESET_DEBUG_FILE", "yes", 1);
			break;
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if(worker_mode != WORKER_MODE_FOREMAN && foreman_name) {
		if(foreman_name) { // for backwards compatibility with the old syntax for specifying a worker's project name
			list_push_tail(preferred_masters, foreman_name);
		}
	}

	check_arguments(argc, argv);

	signal(SIGTERM, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGINT, handle_abort);
	signal(SIGCHLD, handle_sigchld);

	random_init();
	bad_masters = hash_cache_create(127, hash_string, (hash_cache_cleanup_t)free_work_queue_master);
	
	if(!setup_workspace()) {
		fprintf(stderr, "work_queue_worker: failed to setup workspace at %s.\n", workspace);
		exit(1);
	}

	if(terminate_boundary > 0 && idle_timeout > terminate_boundary) {
		idle_timeout = MAX(short_timeout, terminate_boundary - TERMINATE_BOUNDARY_LEEWAY);
	}
	
	if(worker_mode == WORKER_MODE_FOREMAN) {
		char foreman_string[WORK_QUEUE_LINE_MAX];
		sprintf(foreman_string, "%s-foreman", argv[0]);
		debug_config(foreman_string);
		foreman_q = work_queue_create(foreman_port);
		
		if(!foreman_q) {
			fprintf(stderr, "work_queue_worker-foreman: failed to create foreman queue.  Terminating.\n");
			exit(1);
		}
		
		if(foreman_name) {
			work_queue_specify_name(foreman_q, foreman_name);
			work_queue_specify_master_mode(foreman_q, WORK_QUEUE_MASTER_MODE_CATALOG);
		}
		work_queue_specify_estimate_capacity_on(foreman_q, enable_capacity);
		work_queue_activate_fast_abort(foreman_q, fast_abort_multiplier);	
		work_queue_specify_log(foreman_q, foreman_stats_filename);
		
		unfinished_tasks = itable_create(0);
	} else {
		active_tasks = itable_create(0);
		stored_tasks = itable_create(0);
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

		if(worker_mode == WORKER_MODE_FOREMAN) {
			foreman_for_master(master);
		} else {
			work_for_master(master);
		}
	}

abort:
	abort_worker();
	return 0;
}
