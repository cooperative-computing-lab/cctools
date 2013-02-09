/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"
#include "work_queue_protocol.h"

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

#include <unistd.h>
#include <dirent.h>

#include <sys/signal.h>
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

#define MIN_TERMINATE_BOUNDARY 0 
#define TERMINATE_BOUNDARY_LEEWAY 30

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

// Threshold for available disk space (MB) beyond which clean up and restart.
static UINT64_T disk_avail_threshold = 100;

// Terminate only when:
// terminate_boundary - (current_time - worker_start_time)%terminate_boundary ~~ 0
// Use case: hourly charge resource such as Amazon EC2
static int terminate_boundary = 0;

// Basic worker global variables
static char actual_addr[LINK_ADDRESS_MAX];
static int actual_port;
static char workspace[WORKER_WORKSPACE_NAME_MAX];
static char *os_name = NULL; 
static char *arch_name = NULL;
static char *user_specified_workdir = NULL;
static time_t worker_start_time = 0;

// Forked task related
static timestamp_t task_start;
static int task_output_fd = -1;
static const char task_output_template[] = "./worker.stdout.XXXXXX";
static char task_output_name[sizeof(task_output_template)];
static pid_t task_pid = (pid_t)-1;
#define task_running (task_pid != ((pid_t)-1))

// Catalog mode control variables
static char *catalog_server_host = NULL;
static int catalog_server_port = 0;
static int auto_worker = 0;
static char *pool_name = NULL;
static struct work_queue_master *actual_master = NULL;
static struct list *preferred_masters = NULL;
static struct hash_cache *bad_masters = NULL;
static int released_by_master = 0;

static void task_remove_output (void)
{
	int result;
	result = unlink(task_output_name);
	if (result == -1) fatal("could not unlink worker stdout: %s", strerror(errno));
	close(task_output_fd);
	task_output_fd = -1;
}

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

static void make_hash_key(const char *addr, int port, char *key)
{
	sprintf(key, "%s:%d", addr, port);
}

static int have_enough_disk_space() { /* FIXME this should be checked in main loop, if worker output gets too large */
	UINT64_T disk_avail, disk_total;

	// Check available disk space
	if(disk_avail_threshold > 0) {
		disk_info_get(".", &disk_avail, &disk_total);
		if(disk_avail < disk_avail_threshold) {
			debug(D_WQ, "Available disk space (%llu) lower than threshold (%llu).\n", disk_avail, disk_avail_threshold);
			return 0;
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
			link_handle_children(master, 1);

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

	if(!have_enough_disk_space()) {
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
	//make sure a few seconds have passed since child process was created to avoid sending a signal 
	//before it has been fully initialized. Else, the signal sent to that process gets lost.	
	timestamp_t elapsed_time_task_start = timestamp_get() - task_start;
	if (elapsed_time_task_start/1000000 < 3)
		sleep(3 - (elapsed_time_task_start/1000000));	
	
	debug(D_WQ, "terminating the current running task - process %d", task_pid);
	// Send signal to process group of child which is denoted by -ve value of child pid.
	// This is done to ensure delivery of signal to processes forked by the child. 
	kill((-1*task_pid), SIGKILL);
}

static void kill_and_reap_task() {
	if(task_running) {
		pid_t pid;
		int status;
		struct rusage rusage;

		kill_task();

		pid = wait4(task_pid, &status, 0, &rusage);

		assert(pid == task_pid); /* nothing else can be done if this fails... */

		task_pid = (pid_t)-1;
	}
}

static int do_kill() {
	if (task_running)
		kill_task();
	return 1;
}

static int do_release() {
	debug(D_WQ, "released by master at %s:%d.\n", actual_addr, actual_port);
	released_by_master = 1;
	return 0;
}

static int send_keepalive(struct link *master){
	//Respond to keepalive only when running a task. When not running a task,
	//master can determine if I am alive when it tries sending me a task. 
	if(task_running) {
		link_putliteral(master, "alive\n", time(0) + active_timeout);
		debug(D_WQ, "sent response to keepalive check from master at %s:%d.\n", actual_addr, actual_port);
	}
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

static int check_task(struct link *master) {
	int status;
	struct rusage rusage;

	if(task_pid == (pid_t)-1) return 1; /* we have no task running */

	pid_t pid = wait4(task_pid, &status, WNOHANG, &rusage);
	if(pid == (pid_t)0) {
		return 1; /* normal */
	}
	else if (pid == (pid_t)-1) {
		debug(D_WQ, "waitpid failed: %s", strerror(errno));
		task_remove_output();
		return 0; /* something bad has happened */
	}
	else {
		struct stat buf;

		assert(pid == task_pid);
		task_pid = (pid_t)-1;

		int result = fstat(task_output_fd, &buf);
		if (result == -1) {
			debug(D_WQ, "output stat failed: %s", strerror(errno));
			task_remove_output();
			return 0;
		}

		if (!WIFEXITED(status)) {
			debug(D_WQ, "Task (process %d) did not exit normally.\n", pid);
		} 

		time_t execution_time = timestamp_get() - task_start;
		debug(D_WQ, "Task complete: result %d %llu %llu", status, (unsigned long long) buf.st_size, (unsigned long long) execution_time);
		link_putfstring(master, "result %d %llu %llu\n", time(0) + active_timeout, status, (unsigned long long) buf.st_size, (unsigned long long) execution_time);
		if(lseek(task_output_fd, 0, SEEK_SET) == -1) {
			debug(D_WQ, "seek of task stdout failed: %s", strerror(errno));
			task_remove_output();
			return 0;
		}
		link_stream_from_fd(master, task_output_fd, (INT64_T) buf.st_size, time(0)+active_timeout);
		task_remove_output();

		return 1;
	}
}

static void abort_worker() {
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

static pid_t execute_task(const char *cmd)
{
	pid_t pid;
 
	fflush(NULL); /* why is this necessary? */
 
	strcpy(task_output_name, task_output_template);
	task_output_fd = mkstemp(task_output_name);
	if (task_output_fd == -1) fatal("could not open worker stdout: %s", strerror(errno));

	pid = fork();
	if(pid > 0) {
		debug(D_WQ, "started process %d: %s", pid, cmd);

		// Make child process the leader of its own process group. This allows
		// signals to also be delivered to processes forked by the child process.
		// This is currently used by kill_task(). 
		setpgid(pid, 0); 

		return pid;
	} else if (pid == 0){
		int fd = open("/dev/null", O_RDONLY);
		if (fd == -1) fatal("could not open /dev/null: %s", strerror(errno));
		int result = dup2(fd, STDIN_FILENO);
		if (result == -1) fatal("could not dup /dev/null to stdin: %s", strerror(errno));

		result = dup2(task_output_fd, STDOUT_FILENO);
		if (result == -1) fatal("could not dup task_output_fd to stdout: %s", strerror(errno));

		result = dup2(task_output_fd, STDERR_FILENO);
		if (result == -1) fatal("could not dup task_output_fd to stderr: %s", strerror(errno));

		close(task_output_fd);

		execlp("sh", "sh", "-c", cmd, (char *) 0);
		_exit(127);	// Failed to execute the cmd.
	} else {
		debug(D_WQ, "couldn't create new process: %s\n", strerror(errno));
        task_remove_output();
		return 0;
	}
}

static int do_work(struct link *master, INT64_T length) {
	char *cmd = malloc(length + 10);

	link_read(master, cmd, length, time(0) + active_timeout);
	cmd[length] = 0;

	debug(D_WQ, "%s", cmd);
	task_start = timestamp_get();

	task_pid = execute_task(cmd);
	free(cmd);
	if(task_pid == (pid_t)-1) {
		fprintf(stderr, "work_queue_worker: failed to fork task. Shutting down worker...\n");
		return -1;
	}

	return 1;
}

static struct link *connect_master(time_t stoptime) {
	struct link *master = NULL;
	int backoff_multiplier = 2; 
	int backoff_interval = init_backoff_interval;

	while(!abort_flag) {
		if(stoptime < time(0)) {
			// Failed to connect to any master.
			if(terminate_boundary > 0) {
				time_t elapsed_time = time(0) - worker_start_time;
				int time_to_boundary = terminate_boundary - (elapsed_time % terminate_boundary);
				debug(D_DEBUG, "Elapsed time: %ld sec. Terminate boundary: %d sec. Time to next terminate boundary: %d sec.", elapsed_time, terminate_boundary, time_to_boundary);
				if(time_to_boundary < TERMINATE_BOUNDARY_LEEWAY) { 
					// the $TERMINATE_BOUNDARY_LEEWAY seconds is just to allow some extra time to process the shutdown of the worker so that we don't surpass the boundary 
					debug(D_NOTICE, "work_queue_worker: giving up because couldn't connect to any master in %d seconds and terminate boundary almost reached.\n", idle_timeout);
					break;
				}
			} else {
				if(auto_worker) {
					debug(D_NOTICE, "work_queue_worker: giving up because couldn't connect to any master in %d seconds.\n", idle_timeout);
				} else {
					debug(D_NOTICE, "work_queue_worker: giving up because couldn't connect to %s:%d in %d seconds.\n", actual_addr, actual_port, idle_timeout);
				}
				break;
			}
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

		link_handle_children(master, 1);
		link_tune(master, LINK_TUNE_INTERACTIVE);
		report_worker_ready(master);

		//reset backoff interval after connection to master.
		backoff_interval = init_backoff_interval; 

		return master;
	}

	return NULL;
}

static int handle_link(struct link *master) {
	char line[WORK_QUEUE_LINE_MAX];
	char filename[WORK_QUEUE_LINE_MAX];
	char path[WORK_QUEUE_LINE_MAX];
	INT64_T length;
	int status;
	int mode;

	status = link_readline(master, line, sizeof(line), time(0)+short_timeout);
	if(status > 0) {
		debug(D_WQ, "received command: %s.\n", line);
		if(sscanf(line, "work %lld", &length)) {
			status = do_work(master, length);
		} else if(sscanf(line, "stat %s", filename) == 1) {
			status = do_stat(master, filename);
		} else if(sscanf(line, "symlink %s %s", path, filename) == 2) {
			status = do_symlink(path, filename);
		} else if(sscanf(line, "put %s %lld %o", filename, &length, &mode) == 3) {
			if(path_within_workspace(filename, workspace)) {
				status = do_put(master, filename, length, mode);
			} else {
				debug(D_WQ, "Path - %s is not within workspace %s.", filename, workspace);
				status = 0;
			}
		} else if(sscanf(line, "unlink %s", path) == 1) {
			if(path_within_workspace(filename, workspace)) {
				status = do_unlink(path);
			} else {
				debug(D_WQ, "Path - %s is not within workspace %s.", filename, workspace);
				status = 0;
			}
		} else if(sscanf(line, "mkdir %s %o", filename, &mode) == 2) {
			status = do_mkdir(filename, mode);
		} else if(sscanf(line, "rget %s", filename) == 1) {
			status = do_rget(master, filename);
		} else if(sscanf(line, "get %s", filename) == 1) {	// for backward compatibility
			status = do_get(master, filename);
		} else if(sscanf(line, "thirdget %d %s %[^\n]", &mode, filename, path) == 3) {
			status = do_thirdget(mode, filename, path);
		} else if(sscanf(line, "thirdput %d %s %[^\n]", &mode, filename, path) == 3) {
			status = do_thirdput(master, mode, filename, path);
		} else if(!strncmp(line, "kill", 5)){
			status = do_kill();
		} else if(!strncmp(line, "release", 8)) {
			status = do_release();
		} else if(!strncmp(line, "exit", 5)) {
			kill_and_reap_task();
			status = 0;
		} else if(!strncmp(line, "check", 6)) {
			status = send_keepalive(master);
		} else {
			debug(D_WQ, "Unrecognized master message: %s.\n", line);
			status = 0;
		}
	} else if (status == 0 || (status == -1 && errno == EINTR)) {
		status = 1;
	} else if (status == -1) {
		debug(D_WQ, "Failed to read from master: %s", strerror(errno));
		status = 0;
	}

	return status;
}

static void work_for_master(struct link *master) {
	debug(D_WQ, "Working for master at %s:%d.\n", actual_addr, actual_port);

	time_t idle_stoptime = time(0) + idle_timeout;
	// Start serving masters
	while(!abort_flag) {
		if(time(0) > idle_stoptime && !task_running) {
			debug(D_NOTICE, "work_queue_worker: giving up because did not receive any task in %d seconds.\n", idle_timeout);
			abort_flag = 1;
			break;
		}

		if(!check_task(master)) {
			abort_flag = 1;
			break;
		}

        /* There is a race condition for SIGCHLD between check_task and when
         * the actual read or poll occurs in link.c. If SIGCHLD is received
         * between waitpid and read/poll, the system call is not interrupted
         * and we wait the full timeout. This race condition also existed when
         * we previously polled the process stdout (pipe) and the link file
         * descriptor. To fix it, you need to use siglongjmp/sigprocmask to
         * properly catch the signal between the system calls. This is
         * complicated and despite being POSIX.1, might not be portable.
         */
		if(!handle_link(master)) {
			disconnect_master(master);
			break;
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
	fprintf(stdout, " -B <time>      Set the worker to terminate itself only when the elapsed time is multiples of <time>.\n");
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

	debug_config(argv[0]);

	while((c = getopt(argc, argv, "aC:B:d:t:o:p:N:w:i:b:z:A:O:s:vh")) != (char) -1) {
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
			disk_avail_threshold = string_metric_parse(optarg);
			disk_avail_threshold *= 1024 * 1024; //convert MB to Bytes.
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

	{
		sigset_t mask;
		sigemptyset(&mask);
		sigaddset(&mask, SIGCHLD);
		if (sigprocmask(SIG_BLOCK, &mask, NULL) == -1)
			fatal("could not block SIGCHLD: %s", strerror(errno));
	}

	srand((unsigned int) (getpid() ^ time(NULL)));
	bad_masters = hash_cache_create(127, hash_string, (hash_cache_cleanup_t)free_work_queue_master);
	
	if(!setup_workspace()) {
		fprintf(stderr, "work_queue_worker: failed to setup workspace at %s.\n", workspace);
		exit(1);
	}

	if(terminate_boundary > 0 && idle_timeout > terminate_boundary) {
		idle_timeout = MAX(short_timeout, terminate_boundary - TERMINATE_BOUNDARY_LEEWAY);
	}

	// set $WORK_QUEUE_SANDBOX to workspace.
	debug(D_WQ, "WORK_QUEUE_SANDBOX set to %s.\n", workspace);
	setenv("WORK_QUEUE_SANDBOX", workspace, 0);

	// change to workspace
	chdir(workspace);

	if(!have_enough_disk_space()) {
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
