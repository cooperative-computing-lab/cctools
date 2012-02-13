/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"

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
#include "xmalloc.h"
#include "debug.h"
#include "stringtools.h"
#include "load_average.h"
#include "domain_name_cache.h"
#include "getopt.h"
#include "full_io.h"
#include "create_dir.h"

#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <math.h>
#include <signal.h>
#include <dirent.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/signal.h>
#include <sys/utsname.h>
#include <sys/resource.h>
#include <sys/wait.h>

struct task_info {
	pid_t pid;
	int status; 
	struct rusage rusage;
	timestamp_t execution_end;
	char *output;
	INT64_T output_length;
};

// Maximum time to wait before aborting if there is no connection to the master.
static int idle_timeout = 900;

// Maxium time to wait before switching to another master.
static int master_timeout = 60;

// Maximum time to wait when actively communicating with the master.
static int active_timeout = 3600;

// Flag gets set on receipt of a terminal signal.
static int abort_flag = 0;

// Catalog mode control variables
static int auto_worker = 0;
static int exclusive_worker = 1;
static const int non_preference_priority_max = 100;
static char *catalog_server_host = NULL;
static int catalog_server_port = 0;
static struct work_queue_master *actual_master = NULL;

static struct list *preferred_masters = NULL;
static struct hash_cache *bad_masters = NULL;

static int pipefds[2];


static void alarm_handler(int sig)
{
	// do nothing except interrupt the wait
}

void report_worker_ready(struct link *master) {
	char hostname[DOMAIN_NAME_MAX];
	UINT64_T memory_avail, memory_total;
	UINT64_T disk_avail, disk_total;
	int ncpus;
	struct utsname uname_data;
	char *pm;
	char preferred_master_names[WORK_QUEUE_LINE_MAX];


	domain_name_cache_guess(hostname);
	ncpus = load_average_get_cpus();
	memory_info_get(&memory_avail, &memory_total);
	disk_info_get(".", &disk_avail, &disk_total);
	uname(&uname_data);

	if(exclusive_worker) {
		preferred_master_names[0] = 0;
		list_first_item(preferred_masters);
		while((pm = (char *) list_next_item(preferred_masters))) {
			sprintf(&(preferred_master_names[strlen(preferred_master_names)]), "%s ", pm);
		}

		link_putfstring(master, "ready %s %d %llu %llu %llu %llu \"%s\" %s %s\n", time(0) + active_timeout, hostname, ncpus, memory_avail, memory_total, disk_avail, disk_total, preferred_master_names, uname_data.sysname, uname_data.machine);
	} else {
		link_putfstring(master, "ready %s %d %llu %llu %llu %llu %s %s\n", time(0) + active_timeout, hostname, ncpus, memory_avail, memory_total, disk_avail, disk_total, uname_data.sysname, uname_data.machine);
	}

}

pid_t execute_task(const char *cmd) {
	pid_t pid;

	fflush(NULL);

    if (pipe(pipefds) == -1) {
		debug(D_WQ, "Failed to create pipe for output redirecting.\n");
		return 0;
	}

	pid = fork();
	if(pid > 0) {
		debug(D_WQ, "started process %d: %s", pid, cmd);
		close(pipefds[1]);
		return pid;
	} else if(pid < 0) {
		debug(D_WQ, "couldn't create new process: %s\n", strerror(errno));
		close(pipefds[0]);
		close(pipefds[1]);
		return 0;
	} else {
		close(1);
		dup(pipefds[1]);

		close(pipefds[0]);
		close(pipefds[1]);

		execlp("sh", "sh", "-c", cmd, (char *)0);
		_exit(127);	// Failed to execute the cmd.
	}
	return 0;
}

int wait_task(pid_t pid, int timeout, struct task_info *ti) {
	int flags = 0;
	void *old_handler = 0;
	pid_t tmp_pid;
	int   tmp_status;
	struct rusage tmp_rusage;

	if(pid <= 0 || timeout < 0 || !ti) {
		return 0;
	}

	if(timeout == 0) {
		flags = WNOHANG;
	} else {
		flags = 0;
		old_handler = signal(SIGALRM, alarm_handler);
		alarm(timeout);
	}

	tmp_pid = wait4(pid, &tmp_status, flags, &tmp_rusage);

	if(timeout != 0) {
		alarm(0);
		signal(SIGALRM, old_handler);
	}

	if(tmp_pid <= 0) {
		return 0;
	}

	ti->pid = tmp_pid;
	ti->status = tmp_status;
	ti->rusage = tmp_rusage;
	ti->execution_end= timestamp_get();

	// Record stdout of the child process
	char *output;
	INT64_T length;
	FILE *stream = fdopen(pipefds[0], "r");
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
		debug(D_WQ, "Couldn't open pipe fd as stream (failed to read child process's output).\n", strerror(errno));
		length = 0;
		output = NULL;
		close(pipefds[0]);
	}

	ti->output = output;
	ti->output_length = length;

	return 1;
}

void clear_task_info(struct task_info *ti) {
	free(ti->output);
	memset(ti, 0, sizeof(ti));
}

void report_task_complete(struct link *master, int result, char *output, INT64_T output_length, timestamp_t execution_time) {
	debug(D_WQ, "result %d %lld %llu", result, output_length, execution_time);
	link_putfstring(master, "result %d %lld %llu\n", time(0) + active_timeout, result, output_length, execution_time);
	link_putlstring(master, output, output_length, time(0) + active_timeout);
}

static void make_hash_key(const char *addr, int port, char *key)
{
	sprintf(key, "%s:%d", addr, port);
}

/**
 * Reasons for a master being bad:
 * 1. The master does not need more workers right now;
 * 2. The master is already shut down but its record is still in the catalog server.
 */
static void record_bad_master(struct work_queue_master *m)
{
	char key[LINK_ADDRESS_MAX + 10];	// addr:port
	int lifetime = 10;

	if(!m)
		return;

	make_hash_key(m->addr, m->port, key);
	hash_cache_insert(bad_masters, key, m, lifetime);
	debug(D_WQ, "Master at %s:%d is not receiving more workers.\nWon't connect to this master in %d seconds.", m->addr, m->port, lifetime);
}

struct list *get_work_queue_masters(const char *catalog_host, int catalog_port)
{
	struct catalog_query *q;
	struct nvpair *nv;
	struct list *ml;
	struct work_queue_master *m;
	char *pm;
	time_t timeout = 60, stoptime;
	char key[LINK_ADDRESS_MAX + 10];	// addr:port

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

			list_first_item(preferred_masters);
			while((pm = (char *) list_next_item(preferred_masters))) {
				if(whole_string_match_regex(m->proj, pm)) {
					// preferred master found
					break;
				}
			}

			if(pm) {
				// This is a preferred master
				m->priority += non_preference_priority_max;
			} else {
				// Master name does not match any preferred master names
				if(exclusive_worker) {
					continue;
				} else {
					m->priority = non_preference_priority_max < m->priority ? non_preference_priority_max : m->priority;
				}
			}

			// exclude 'bad' masters
			make_hash_key(m->addr, m->port, key);
			if(!hash_cache_lookup(bad_masters, key)) {
				list_push_priority(ml, m, m->priority);
			}
		}
		nvpair_delete(nv);
	}

	// Must delete the query otherwise it would occupy 1 tcp connection forever!
	catalog_query_delete(q);
	return ml;
}

struct distribution_node {
	void *item;
	int weight;
};

void *select_item_by_weight(struct distribution_node distribution[], int n) {
	// A distribution example:
	// struct distribution_node array[3] = { {"A", 20}, {"B", 30}, {"C", 50} };
	int i, w, x, sum;

	sum = 0;
	for(i=0; i < n; i++) {
		w = distribution[i].weight;	
		if(w < 0) return NULL;
		sum += w;
	}

	x = rand() % sum;

	for(i = 0; i < n; i++) {
		x -= distribution[i].weight;
		if(x <= 0) return distribution[i].item;
	}

	return NULL;
}

struct link *auto_link_connect(char *addr, int *port, time_t master_stoptime)
{
	struct link *master = 0;
	struct list *ml;
	struct work_queue_master *m;

	ml = get_work_queue_masters(catalog_server_host, catalog_server_port);
	if(!ml)
		return NULL;
	debug_print_masters(ml);

	list_first_item(ml);
	while((m = (struct work_queue_master *) list_next_item(ml))) {
		master = link_connect(m->addr, m->port, master_stoptime);
		if(master) {
			debug(D_WQ, "Talking to the Master at:\n");
			debug(D_WQ, "addr:\t%s\n", m->addr);
			debug(D_WQ, "port:\t%d\n", m->port);
			debug(D_WQ, "project:\t%s\n", m->proj);
			debug(D_WQ, "priority:\t%d\n", m->priority);
			debug(D_WQ, "\n");

			strncpy(addr, m->addr, LINK_ADDRESS_MAX);
			(*port) = m->port;

			if(actual_master)
				free(actual_master);
			actual_master = duplicate_work_queue_master(m);

			break;
		} else {
			record_bad_master(duplicate_work_queue_master(m));
		}
	}

	list_free(ml);
	list_delete(ml);

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
int stream_output_item(struct link *master, const char *filename)
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

static void handle_abort(int sig)
{
	abort_flag = 1;
}

static void show_version(const char *cmd)
{
	fprintf(stdout, "%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	fprintf(stdout, "Use: %s [options] <masterhost> <port>\n", cmd);
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " -a             Enable auto mode. In this mode the worker would ask a catalog server for available masters.\n");
	fprintf(stdout, " -C <catalog>   Set catalog server to <catalog>. Format: HOSTNAME:PORT \n");
	fprintf(stdout, " -d <subsystem> Enable debugging for this subsystem.\n");
	fprintf(stdout, " -N <project>   Name of a preferred project. A worker can have multiple preferred projects.\n");
	fprintf(stdout, " -s             Run as a shared worker. By default the worker would only work on preferred projects.\n");
	fprintf(stdout, " -t <time>      Abort after this amount of idle time. (default=%ds)\n", idle_timeout);
	fprintf(stdout, " -o <file>      Send debugging to this file.\n");
	fprintf(stdout, " -v             Show version string\n");
	fprintf(stdout, " -w <size>      Set TCP window size.\n");
	fprintf(stdout, " -z <size>      Set available disk space threshold (in MB) before aborting. By default no checks on available space are performed.\n");
	fprintf(stdout, " -h             Show this help screen\n");
}

int main(int argc, char *argv[])
{
	const char *host = NULL;
	int port = WORK_QUEUE_DEFAULT_PORT;
	char actual_addr[LINK_ADDRESS_MAX];
	int actual_port;
	struct link *master = 0;
	char addr[LINK_ADDRESS_MAX];
	UINT64_T disk_avail, disk_total;
	UINT64_T avail_space_threshold = 0;
	char c;
	int w;
	char deletecmd[WORK_QUEUE_LINE_MAX];
	timestamp_t execution_start, execution_end;
	int task_running = 0;
	pid_t pid;
	time_t readline_stoptime;

	preferred_masters = list_create();
	if(!preferred_masters) {
		fprintf(stderr, "Cannot allocate memory to store preferred work queue masters names.\n");
		exit(1);
	}

	debug_config(argv[0]);

	while((c = getopt(argc, argv, "aC:d:ihN:o:st:w:z:v")) != (char) -1) {
		switch (c) {
		case 'a':
			auto_worker = 1;
			break;
		case 'C':
			port = parse_catalog_server_description(optarg, &catalog_server_host, &catalog_server_port);
			if(!port) {
				fprintf(stderr, "The provided catalog server is invalid. The format of the '-s' option should be '-s HOSTNAME:PORT'.\n");
				exit(1);
			}
			auto_worker = 1;
			break;
		case 's':
			auto_worker = 1;
			// This is a shared worker
			exclusive_worker = 0;
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
		case 'v':
			show_version(argv[0]);
			return 0;
		case 'w':
			w = string_metric_parse(optarg);
			link_window_set(w, w);
			break;
		case 'z':
			avail_space_threshold = string_metric_parse(optarg) * 1024 * 1024;
			break;
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	srand((unsigned int)(getpid() ^ time(NULL)));
		
	if(!auto_worker) {
		if((argc - optind) != 2) {
			show_help(argv[0]);
			exit(1);
		}
		host = argv[optind];
		port = atoi(argv[optind + 1]);

		if(!domain_name_cache_lookup(host, addr)) {
			fprintf(stderr, "couldn't lookup address of host %s\n", host);
			exit(1);
		}
	}

	if(auto_worker && exclusive_worker && !list_size(preferred_masters)) {
		fprintf(stderr, "Worker is running under exclusive mode. But no preferred master is specified.\n");
		fprintf(stderr, "Please specify the preferred master names with -N option or add -s option to allow the worker to work for any available masters.\n");
		exit(1);
	}

	signal(SIGTERM, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGINT, handle_abort);

	// Setup working space(dir)
	const char *workdir;
	if(getenv("_CONDOR_SCRATCH_DIR")) {
		workdir = getenv("_CONDOR_SCRATCH_DIR");
	} else {
		workdir = "/tmp";
	}

	char tempdir[WORK_QUEUE_LINE_MAX];
	sprintf(tempdir, "%s/worker-%d-%d", workdir, (int) getuid(), (int) getpid());

	fprintf(stdout, "work_queue_worker: working in %s\n", tempdir);
	mkdir(tempdir, 0700);
	chdir(tempdir);
	
	// Check available disk space
	if (avail_space_threshold > 0) {
		disk_info_get(".", &disk_avail, &disk_total);
		if (disk_avail < avail_space_threshold) {
			fprintf(stderr, "worker: aborting since available disk space (%llu) lower than threshold (%llu).\n", disk_avail, avail_space_threshold);
			goto abort;
		}	
	}	


	time_t idle_stoptime = time(0) + idle_timeout;
	time_t switch_master_time = time(0) + master_timeout;

	bad_masters = hash_cache_create(127, hash_string, (hash_cache_cleanup_t) free);
		
	// Start serving masters
	while(!abort_flag) {
		char line[WORK_QUEUE_LINE_MAX];
		int result, mode, fd;
		INT64_T length;
		char filename[WORK_QUEUE_LINE_MAX];
		char path[WORK_QUEUE_LINE_MAX];
		char *buffer;
		FILE *stream;

		if(time(0) > idle_stoptime) {
			if(master) {
				fprintf(stdout, "work_queue_worker: giving up because did not receive any task in %d seconds.\n", idle_timeout);
			} else {
				if(auto_worker) {
					fprintf(stdout, "work_queue_worker: giving up because couldn't connect to any master in %d seconds.\n", idle_timeout);
				} else {
					fprintf(stdout, "work_queue_worker: giving up because couldn't connect to %s:%d in %d seconds.\n", host, port, idle_timeout);
				}
			}
			break;
		}

		switch_master_time = time(0) + master_timeout;
		if(!master) {
			if(auto_worker) {
				master = auto_link_connect(actual_addr, &actual_port, switch_master_time);
			} else {
				master = link_connect(addr, port, idle_stoptime);
			}
			if(!master) {
				sleep(5);
				continue;
			}

			link_tune(master, LINK_TUNE_INTERACTIVE);

			report_worker_ready(master);
		}

		
		// Wait for forked task's completion
		if(task_running) { 
			struct task_info ti;
			if(wait_task(pid, 5, &ti)) {
				report_task_complete(master, ti.status, ti.output, ti.output_length, ti.execution_end - execution_start);
				clear_task_info(&ti);
				task_running = 0;
			} 
		} 

		if(task_running) {
			readline_stoptime = time(0) + 1;
		} else {
			readline_stoptime = time(0) + active_timeout;
		}

		if(link_readline(master, line, sizeof(line), readline_stoptime)) {
			debug(D_WQ, "%s", line);
			if(sscanf(line, "work %lld", &length)) {
				buffer = malloc(length + 10);
				link_read(master, buffer, length, time(0) + active_timeout);
				buffer[length] = 0;
				strcat(buffer, " 2>&1");
				debug(D_WQ, "%s", buffer);
				execution_start = timestamp_get();

				char string[WORK_QUEUE_LINE_MAX];
				if(sscanf(line, "work %lld %s", &length, string) == 2 && !strncmp(string, "fork", 4)) {
					pid = execute_task(buffer);
					free(buffer);
					if(pid < 0) {
						fprintf(stderr,"work_queue_worker: couldn't submit local job. \nShutting down worker...\n");
						break;
					}
					task_running = 1;
				} else {
					// execute the command
					stream = popen(buffer, "r");
					free(buffer);
					if(stream) {
						length = copy_stream_to_buffer(stream, &buffer);
						if(length < 0)
							length = 0;
						result = pclose(stream);
					} else {
						length = 0;
						result = -1;
						buffer = 0;
					}
					execution_end = timestamp_get();

					// return job done
					report_task_complete(master, result, buffer, length, execution_end-execution_start);

					if(buffer) {
						free(buffer);
					}
				}
			} else if(sscanf(line, "stat %s", filename) == 1) {
				struct stat st;
				if(!stat(filename, &st)) {
					debug(D_WQ, "result 1 %lu %lu", (unsigned long int) st.st_size, (unsigned long int) st.st_mtime);
					link_putfstring(master, "result 1 %lu %lu\n", time(0) + active_timeout, (unsigned long int) st.st_size, (unsigned long int) st.st_mtime);
				} else {
					debug(D_WQ, "result 0 0 0");
					link_putliteral(master, "result 0 0 0\n", time(0) + active_timeout);
				}
			} else if(sscanf(line, "symlink %s %s", path, filename) == 2) {
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
						goto recover;
					}
					*tmp_pos = '/';
				}
				symlink(path, filename);
			} else if(sscanf(line, "put %s %lld %o", filename, &length, &mode) == 3) {
				if (avail_space_threshold > 0) {
					disk_info_get(".", &disk_avail, &disk_total);
					if (disk_avail < avail_space_threshold) {
						debug(D_WQ, "Available disk space (%llu) lower than threshold (%llu).\n", disk_avail, avail_space_threshold);
						goto recover;
					}	
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
						goto recover;
					}
					*tmp_pos = '/';
				}

				fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, mode);
				if(fd < 0)
					goto recover;
				INT64_T actual = link_stream_to_fd(master, fd, length, time(0) + active_timeout);
				close(fd);
				if(actual != length)
					goto recover;
			} else if(sscanf(line, "unlink %s", path) == 1) {
				result = remove(path);
				if(result != 0) {	// 0 - succeeded; otherwise, failed
					fprintf(stderr, "Could not remove file: %s.(%s)\n", path, strerror(errno));
					goto recover;
				}
			} else if(sscanf(line, "mkdir %s %o", filename, &mode) == 2) {
				if(!create_dir(filename, mode | 0700)) {
					debug(D_WQ, "Could not create directory - %s (%s)\n", filename, strerror(errno));
					goto recover;
				}
			} else if(sscanf(line, "rget %s", filename) == 1) {
				stream_output_item(master, filename);
				link_putliteral(master, "end\n", time(0) + active_timeout);
			} else if(sscanf(line, "get %s", filename) == 1) {	// for backward compatibility
				struct stat info;
				if(stat(filename, &info) != 0) {
					fprintf(stderr, "Output file %s was not created. (%s)\n", filename, strerror(errno));
					goto recover;
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
						goto recover;
					}
				} else {
					fprintf(stderr, "Could not open output file %s. (%s)\n", filename, strerror(errno));
					goto recover;
				}
			} else if(sscanf(line, "thirdget %d %s %[^\n]", &mode, filename, path) == 3) {
				char cmd[WORK_QUEUE_LINE_MAX];
				char *cur_pos, *tmp_pos;

				if(mode != WORK_QUEUE_FS_CMD) {
					struct stat info;
					if(stat(path, &info) != 0) {
						debug(D_WQ, "Path %s not accessible. (%s)\n", path, strerror(errno));
						goto recover;
					}
					if(!strcmp(filename, path)) {
						debug(D_WQ, "thirdget aborted: filename (%s) and path (%s) are the same\n", filename, path);
						continue;
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
						goto recover;
					}
					*tmp_pos = '/';
				}

				switch (mode) {
				case WORK_QUEUE_FS_SYMLINK:
					if(symlink(path, filename) != 0) {
						debug(D_WQ, "Could not thirdget %s, symlink (%s) failed. (%s)\n", filename, path, strerror(errno));
						goto recover;
					}
				case WORK_QUEUE_FS_PATH:
					sprintf(cmd, "/bin/cp %s %s", path, filename);
					if(system(cmd) != 0) {
						debug(D_WQ, "Could not thirdget %s, copy (%s) failed. (/bin/cp %s)\n", filename, path, filename, strerror(errno));
						goto recover;
					}
					break;
				case WORK_QUEUE_FS_CMD:
					if(system(path) != 0) {
						debug(D_WQ, "Could not thirdget %s, command (%s) failed. (%s)\n", filename, path, strerror(errno));
						goto recover;
					}
					break;
				}
			} else if(sscanf(line, "thirdput %d %s %[^\n]", &mode, filename, path) == 3) {
				struct stat info;
				char cmd[WORK_QUEUE_LINE_MAX];

				switch (mode) {
				case WORK_QUEUE_FS_SYMLINK:
				case WORK_QUEUE_FS_PATH:
					if(stat(filename, &info) != 0) {
						debug(D_WQ, "File %s not accessible. (%s)\n", filename, strerror(errno));
						goto recover;
					}
					if(!strcmp(filename, path)) {
						debug(D_WQ, "thirdput aborted: filename (%s) and path (%s) are the same\n", filename, path);
						continue;
					}
					sprintf(cmd, "/bin/cp %s %s", filename, path);
					if(system(cmd) != 0) {
						debug(D_WQ, "Could not thirdput %s, copy (%s) failed. (%s)\n", filename, path, strerror(errno));
						goto recover;
					}
					break;
				case WORK_QUEUE_FS_CMD:
					if(system(path) != 0) {
						debug(D_WQ, "Could not thirdput %s, command (%s) failed. (%s)\n", filename, path, strerror(errno));
						goto recover;
					}
					break;
				}
				link_putliteral(master, "thirdput complete\n", time(0) + active_timeout);
			} else if(!strncmp(line, "exit", 4)) {
				break;
			} else {
				link_putliteral(master, "error\n", time(0) + active_timeout);
			}

			idle_stoptime = time(0) + idle_timeout;

		} else {
recover:
			link_close(master);
			master = 0;
			if(auto_worker) {
				record_bad_master(duplicate_work_queue_master(actual_master));
			}
			sprintf(deletecmd, "rm -rf %s/*", tempdir);
			system(deletecmd);
			sleep(5);
		}
	}

abort:	
	fprintf(stdout, "work_queue_worker: cleaning up %s\n", tempdir);
	sprintf(deletecmd, "rm -rf %s", tempdir);
	system(deletecmd);

	return 0;
}
